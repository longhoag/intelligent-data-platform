"""
Feature Store REST API Server - Day 4
High-performance feature serving API with caching and monitoring
"""

import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, HTTPException, Query  # type: ignore
from fastapi.middleware.cors import CORSMiddleware  # type: ignore
from pydantic import BaseModel, Field  # type: ignore
from loguru import logger
import uvicorn  # type: ignore
import prometheus_client  # type: ignore
from prometheus_client import Counter, Histogram, Gauge  # type: ignore

from .store import FeatureStore


# Pydantic models for API
class FeatureRequest(BaseModel):
    """Request model for feature serving"""
    feature_names: List[str] = Field(..., description="List of feature names to retrieve")
    entity_key: str = Field(..., description="Entity key (e.g., 'symbol')")
    entity_value: str = Field(..., description="Entity value (e.g., 'AAPL')")


class BatchFeatureRequest(BaseModel):
    """Request model for batch feature serving"""
    feature_names: List[str]
    entities: List[Dict[str, str]]  # List of entity key-value pairs


class FeatureResponse(BaseModel):
    """Response model for feature serving"""
    features: Dict[str, Any]
    entity_key: str
    entity_value: str
    timestamp: datetime
    latency_ms: float


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    components: Dict[str, bool]
    timestamp: datetime


class MetricsResponse(BaseModel):
    """Performance metrics response"""
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    cache_hit_ratio: float
    total_requests: int


# Prometheus metrics
REQUEST_COUNT = Counter('feature_requests_total', 'Total feature requests', ['endpoint', 'status'])
REQUEST_LATENCY = Histogram('feature_request_duration_seconds', 'Feature request latency')
CACHE_HITS = Counter('feature_cache_hits_total', 'Cache hits')
CACHE_MISSES = Counter('feature_cache_misses_total', 'Cache misses')
ACTIVE_CONNECTIONS = Gauge('feature_server_active_connections', 'Active connections')


class FeatureServer:
    """High-performance feature serving API"""
    
    def __init__(self, feature_store: FeatureStore, host: str = "0.0.0.0", port: int = 8001):
        self.feature_store = feature_store
        self.host = host
        self.port = port
        self.app = FastAPI(
            title="Intelligent Data Platform - Feature Store API",
            description="Production feature serving with sub-100ms latency",
            version="1.0.0"
        )
        
        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        self._setup_routes()
        logger.info(f"Feature server initialized on {host}:{port}")
    
    def _setup_routes(self):
        """Setup API routes"""
        
        @self.app.get("/", response_model=Dict[str, str])
        async def root():
            """Root endpoint"""
            return {
                "service": "Feature Store API",
                "version": "1.0.0",
                "status": "running"
            }
        
        @self.app.get("/health", response_model=HealthResponse)
        async def health_check():
            """Health check endpoint"""
            try:
                health = self.feature_store.health_check()
                status = "healthy" if all(health.values()) else "degraded"
                
                return HealthResponse(
                    status=status,
                    components=health,
                    timestamp=datetime.now()
                )
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                raise HTTPException(status_code=500, detail="Health check failed") from e
        
        @self.app.get("/metrics/prometheus")
        async def prometheus_metrics():
            """Prometheus metrics endpoint"""
            return prometheus_client.generate_latest()
        
        @self.app.get("/metrics", response_model=MetricsResponse)
        async def get_metrics():
            """Get performance metrics"""
            try:
                metrics = self.feature_store.get_performance_metrics()
                return MetricsResponse(**metrics)
            except Exception as e:
                logger.error(f"Metrics retrieval failed: {e}")
                raise HTTPException(status_code=500, detail="Metrics unavailable") from e
        
        @self.app.post("/features/online", response_model=FeatureResponse)
        async def get_online_features(request: FeatureRequest):
            """Get features from online store with low latency"""
            start_time = time.time()
            
            try:
                # Validate feature names
                if not request.feature_names:
                    raise HTTPException(status_code=400, detail="Feature names required")
                
                # Get features from store
                features = await self.feature_store.get_online_features(
                    request.feature_names,
                    request.entity_key,
                    request.entity_value
                )
                
                latency_ms = (time.time() - start_time) * 1000
                
                # Record metrics
                REQUEST_COUNT.labels(endpoint='online', status='success').inc()
                REQUEST_LATENCY.observe(time.time() - start_time)
                
                if features:
                    CACHE_HITS.inc()
                else:
                    CACHE_MISSES.inc()
                
                return FeatureResponse(
                    features=features,
                    entity_key=request.entity_key,
                    entity_value=request.entity_value,
                    timestamp=datetime.now(),
                    latency_ms=latency_ms
                )
                
            except Exception as e:
                REQUEST_COUNT.labels(endpoint='online', status='error').inc()
                logger.error(f"Online feature serving failed: {e}")
                raise HTTPException(status_code=500, detail=str(e)) from e
        
        @self.app.post("/features/batch")
        async def get_batch_features(request: BatchFeatureRequest):
            """Get features for multiple entities"""
            start_time = time.time()
            
            try:
                results = []
                
                # Process each entity
                for entity in request.entities:
                    if len(entity) != 1:
                        continue  # Skip malformed entities
                    
                    entity_key, entity_value = next(iter(entity.items()))
                    
                    features = await self.feature_store.get_online_features(
                        request.feature_names,
                        entity_key,
                        entity_value
                    )
                    
                    results.append({
                        'entity_key': entity_key,
                        'entity_value': entity_value,
                        'features': features
                    })
                
                latency_ms = (time.time() - start_time) * 1000
                REQUEST_COUNT.labels(endpoint='batch', status='success').inc()
                
                return {
                    'results': results,
                    'total_entities': len(request.entities),
                    'latency_ms': latency_ms,
                    'timestamp': datetime.now()
                }
                
            except Exception as e:
                REQUEST_COUNT.labels(endpoint='batch', status='error').inc()
                logger.error(f"Batch feature serving failed: {e}")
                raise HTTPException(status_code=500, detail=str(e)) from e
        
        @self.app.get("/features/search")
        async def search_features(
            query: str = Query(..., description="Search term for feature names"),
            limit: int = Query(10, description="Maximum number of results")
        ):
            """Search available features"""
            try:
                # Simple search in feature definitions
                matching_features = []
                
                for name, definition in self.feature_store.feature_definitions.items():
                    if (query.lower() in name.lower() or 
                        query.lower() in definition.description.lower()):
                        matching_features.append({
                            'name': name,
                            'type': definition.feature_type,
                            'description': definition.description,
                            'version': definition.version,
                            'tags': definition.tags
                        })
                
                # Limit results
                matching_features = matching_features[:limit]
                
                return {
                    'query': query,
                    'total_found': len(matching_features),
                    'features': matching_features
                }
                
            except Exception as e:
                logger.error(f"Feature search failed: {e}")
                raise HTTPException(status_code=500, detail=str(e)) from e
        
        @self.app.get("/features/definitions")
        async def list_feature_definitions(
            feature_view: Optional[str] = Query(None, description="Filter by feature view")
        ):
            """List all feature definitions"""
            try:
                definitions = []
                
                for name, definition in self.feature_store.feature_definitions.items():
                    # Filter by feature view if specified
                    if feature_view:
                        view = self.feature_store.feature_views.get(feature_view)
                        if not view or not any(f.name == name for f in view.features):
                            continue
                    
                    definitions.append({
                        'name': name,
                        'type': definition.feature_type,
                        'description': definition.description,
                        'source_table': definition.source_table,
                        'source_column': definition.source_column,
                        'version': definition.version,
                        'created_at': definition.created_at.isoformat(),
                        'tags': definition.tags
                    })
                
                return {
                    'total_features': len(definitions),
                    'feature_view': feature_view,
                    'features': definitions
                }
                
            except Exception as e:
                logger.error(f"Feature definitions listing failed: {e}")
                raise HTTPException(status_code=500, detail=str(e)) from e
        
        @self.app.get("/features/views")
        async def list_feature_views():
            """List all feature views"""
            try:
                views = []
                
                for name, view in self.feature_store.feature_views.items():
                    views.append({
                        'name': name,
                        'entities': view.entities,
                        'feature_count': len(view.features),
                        'ttl_hours': view.ttl.total_seconds() / 3600,
                        'online_enabled': view.online_enabled,
                        'offline_enabled': view.offline_enabled,
                        'batch_source': view.batch_source,
                        'stream_source': view.stream_source
                    })
                
                return {
                    'total_views': len(views),
                    'feature_views': views
                }
                
            except Exception as e:
                logger.error(f"Feature views listing failed: {e}")
                raise HTTPException(status_code=500, detail=str(e)) from e
    
    async def start_server(self):
        """Start the feature server"""
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level="info",
            access_log=True
        )
        server = uvicorn.Server(config)
        
        logger.info(f"Starting feature server on {self.host}:{self.port}")
        await server.serve()
    
    def run(self):
        """Run the feature server (blocking)"""
        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            log_level="info"
        )
