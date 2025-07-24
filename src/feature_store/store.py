"""
Production Feature Store Implementation - Day 4
Core feature store with online/offline serving and point-in-time correctness
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from loguru import logger
import redis  # type: ignore
import psycopg2  # type: ignore
from psycopg2.extras import RealDictCursor  # type: ignore
import json
import hashlib
from dataclasses import dataclass


@dataclass
class FeatureDefinition:
    """Feature definition with metadata"""
    name: str
    feature_type: str  # 'numeric', 'categorical', 'boolean', 'timestamp'
    description: str
    source_table: str
    source_column: str
    transformation: Optional[str] = None
    version: str = "1.0.0"
    created_at: datetime = None
    tags: List[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.tags is None:
            self.tags = []


@dataclass
class FeatureView:
    """Feature view configuration"""
    name: str
    features: List[FeatureDefinition]
    entities: List[str]  # Primary keys
    ttl: timedelta = timedelta(days=7)  # Time-to-live
    batch_source: str = None
    stream_source: str = None
    online_enabled: bool = True
    offline_enabled: bool = True


class FeatureStore:
    """Production feature store with online/offline serving"""
    
    def __init__(self, 
                 redis_host: str = 'localhost',
                 redis_port: int = 6379,
                 postgres_config: Dict[str, str] = None):
        
        # Redis for online serving (low-latency cache)
        self.redis_client = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            decode_responses=True,
            socket_keepalive=True,
            socket_keepalive_options={},
            health_check_interval=30
        )
        
        # PostgreSQL for offline storage and metadata
        self.postgres_config = postgres_config or {
            'host': 'localhost',
            'port': '5432',
            'dbname': 'intelligent_platform',
            'user': 'platform_user',
            'password': 'platform_pass'
        }
        
        # Feature registry
        self.feature_views: Dict[str, FeatureView] = {}
        self.feature_definitions: Dict[str, FeatureDefinition] = {}
        
        # Performance metrics
        self.serving_latency = []
        self.cache_hit_ratio = []
        
        logger.info("Feature store initialized")
        self._initialize_storage()
    
    def _initialize_storage(self):
        """Initialize storage backends"""
        try:
            # Test Redis connection
            self.redis_client.ping()
            logger.info("Redis connection established")
            
            # Test PostgreSQL connection
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            logger.info("PostgreSQL connection established")
            
            # Create feature store tables
            self._create_feature_tables()
            
        except Exception as e:
            logger.error(f"Storage initialization failed: {e}")
            raise
    
    def _create_feature_tables(self):
        """Create feature store metadata tables"""
        with psycopg2.connect(**self.postgres_config) as conn:
            with conn.cursor() as cur:
                # Feature definitions table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS feature_definitions (
                        name VARCHAR(255) PRIMARY KEY,
                        feature_type VARCHAR(50),
                        description TEXT,
                        source_table VARCHAR(255),
                        source_column VARCHAR(255),
                        transformation TEXT,
                        version VARCHAR(50),
                        created_at TIMESTAMP,
                        tags TEXT[]
                    )
                """)
                
                # Feature views table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS feature_views (
                        name VARCHAR(255) PRIMARY KEY,
                        entities TEXT[],
                        ttl_seconds INTEGER,
                        batch_source VARCHAR(255),
                        stream_source VARCHAR(255),
                        online_enabled BOOLEAN,
                        offline_enabled BOOLEAN,
                        created_at TIMESTAMP
                    )
                """)
                
                # Feature values table for offline storage
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS feature_values (
                        feature_name VARCHAR(255),
                        entity_key VARCHAR(255),
                        entity_value VARCHAR(255),
                        feature_value JSONB,
                        event_timestamp TIMESTAMP,
                        created_timestamp TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (feature_name, entity_key, entity_value, event_timestamp)
                    )
                """)
                
                # Create indexes for performance
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_feature_values_entity 
                    ON feature_values (entity_key, entity_value)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_feature_values_timestamp 
                    ON feature_values (event_timestamp)
                """)
                
                conn.commit()
                logger.info("Feature store tables created")
    
    def register_feature_view(self, feature_view: FeatureView):
        """Register a new feature view"""
        try:
            # Store in memory
            self.feature_views[feature_view.name] = feature_view
            
            # Store features in registry
            for feature in feature_view.features:
                self.feature_definitions[feature.name] = feature
            
            # Persist to database
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    # Store feature view
                    cur.execute("""
                        INSERT INTO feature_views 
                        (name, entities, ttl_seconds, batch_source, stream_source, 
                         online_enabled, offline_enabled, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (name) DO UPDATE SET
                        entities = EXCLUDED.entities,
                        ttl_seconds = EXCLUDED.ttl_seconds,
                        batch_source = EXCLUDED.batch_source,
                        stream_source = EXCLUDED.stream_source,
                        online_enabled = EXCLUDED.online_enabled,
                        offline_enabled = EXCLUDED.offline_enabled
                    """, (
                        feature_view.name,
                        feature_view.entities,
                        int(feature_view.ttl.total_seconds()),
                        feature_view.batch_source,
                        feature_view.stream_source,
                        feature_view.online_enabled,
                        feature_view.offline_enabled,
                        datetime.now()
                    ))
                    
                    # Store feature definitions
                    for feature in feature_view.features:
                        cur.execute("""
                            INSERT INTO feature_definitions 
                            (name, feature_type, description, source_table, source_column,
                             transformation, version, created_at, tags)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (name) DO UPDATE SET
                            feature_type = EXCLUDED.feature_type,
                            description = EXCLUDED.description,
                            source_table = EXCLUDED.source_table,
                            source_column = EXCLUDED.source_column,
                            transformation = EXCLUDED.transformation,
                            version = EXCLUDED.version,
                            tags = EXCLUDED.tags
                        """, (
                            feature.name,
                            feature.feature_type,
                            feature.description,
                            feature.source_table,
                            feature.source_column,
                            feature.transformation,
                            feature.version,
                            feature.created_at,
                            feature.tags
                        ))
                    
                    conn.commit()
            
            logger.info(f"Feature view '{feature_view.name}' registered with {len(feature_view.features)} features")
            
        except Exception as e:
            logger.error(f"Failed to register feature view: {e}")
            raise
    
    async def get_online_features(self, 
                                  feature_names: List[str],
                                  entity_key: str,
                                  entity_value: str) -> Dict[str, Any]:
        """Get features from online store with sub-100ms latency"""
        start_time = time.time()
        
        try:
            # Generate cache key
            cache_key = self._generate_cache_key(feature_names, entity_key, entity_value)
            
            # Try cache first
            cached_result = self.redis_client.get(cache_key)
            if cached_result:
                result = json.loads(cached_result)
                latency = (time.time() - start_time) * 1000
                self.serving_latency.append(latency)
                self.cache_hit_ratio.append(1)
                logger.debug(f"Cache hit for {entity_key}={entity_value}, latency: {latency:.2f}ms")
                return result
            
            # Cache miss - fetch from storage
            features = {}
            
            # Use Redis pipeline for multiple feature fetch
            pipe = self.redis_client.pipeline()
            feature_keys = []
            
            for feature_name in feature_names:
                feature_key = f"feature:{feature_name}:{entity_key}:{entity_value}"
                feature_keys.append(feature_key)
                pipe.get(feature_key)
            
            redis_results = pipe.execute()
            
            # Process Redis results
            for feature_name, redis_result in zip(feature_names, redis_results):
                if redis_result:
                    features[feature_name] = json.loads(redis_result)
                else:
                    # Fallback to database
                    db_value = await self._get_feature_from_db(feature_name, entity_key, entity_value)
                    if db_value is not None:
                        features[feature_name] = db_value
                        # Cache for future requests
                        feature_key = f"feature:{feature_name}:{entity_key}:{entity_value}"
                        self.redis_client.setex(
                            feature_key, 
                            3600,  # 1 hour TTL
                            json.dumps(db_value)
                        )
            
            # Cache the combined result
            self.redis_client.setex(cache_key, 300, json.dumps(features))  # 5 min TTL
            
            latency = (time.time() - start_time) * 1000
            self.serving_latency.append(latency)
            self.cache_hit_ratio.append(0)
            
            logger.debug(f"Features served for {entity_key}={entity_value}, latency: {latency:.2f}ms")
            return features
            
        except (redis.RedisError, psycopg2.Error, json.JSONDecodeError) as e:
            logger.error(f"Failed to get online features: {e}")
            return {}
    
    async def _get_feature_from_db(self, feature_name: str, entity_key: str, entity_value: str) -> Any:
        """Get feature value from database with point-in-time correctness"""
        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Get latest feature value before current time
                    cur.execute("""
                        SELECT feature_value, event_timestamp
                        FROM feature_values
                        WHERE feature_name = %s 
                        AND entity_key = %s 
                        AND entity_value = %s
                        AND event_timestamp <= NOW()
                        ORDER BY event_timestamp DESC
                        LIMIT 1
                    """, (feature_name, entity_key, entity_value))
                    
                    result = cur.fetchone()
                    return result['feature_value'] if result else None
                    
        except (psycopg2.Error, KeyError) as e:
            logger.error(f"Database feature lookup failed: {e}")
            return None
    
    def write_features(self, 
                      feature_name: str,
                      entity_key: str, 
                      entity_value: str,
                      feature_value: Any,
                      event_timestamp: datetime = None):
        """Write feature to both online and offline stores"""
        if event_timestamp is None:
            event_timestamp = datetime.now()
        
        try:
            # Write to Redis (online store)
            redis_key = f"feature:{feature_name}:{entity_key}:{entity_value}"
            self.redis_client.setex(
                redis_key,
                3600,  # 1 hour TTL
                json.dumps(feature_value)
            )
            
            # Write to PostgreSQL (offline store)
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO feature_values 
                        (feature_name, entity_key, entity_value, feature_value, event_timestamp)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (feature_name, entity_key, entity_value, event_timestamp)
                        DO UPDATE SET feature_value = EXCLUDED.feature_value
                    """, (
                        feature_name,
                        entity_key, 
                        entity_value,
                        json.dumps(feature_value),
                        event_timestamp
                    ))
                    conn.commit()
            
            logger.debug(f"Feature written: {feature_name}={feature_value}")
            
        except (redis.RedisError, psycopg2.Error, ValueError) as e:
            logger.error(f"Failed to write feature: {e}")
            raise
    
    def get_offline_features(self,
                           feature_names: List[str],
                           entity_df: pd.DataFrame,
                           event_timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """Get historical features for training with point-in-time correctness"""
        try:
            results = []
            
            for _, row in entity_df.iterrows():
                row_features = dict(row)
                event_time = row[event_timestamp_col]
                
                for feature_name in feature_names:
                    # Extract entity key/value from row
                    feature_def = self.feature_definitions.get(feature_name)
                    if not feature_def:
                        continue
                    
                    # For simplicity, assume single entity key
                    entity_key = 'symbol'  # Financial domain assumption
                    entity_value = row.get(entity_key, str(row.iloc[0]))
                    
                    # Get point-in-time feature value
                    feature_value = self._get_historical_feature(
                        feature_name, entity_key, entity_value, event_time
                    )
                    
                    row_features[feature_name] = feature_value
                
                results.append(row_features)
            
            return pd.DataFrame(results)
            
        except (psycopg2.Error, pd.errors.DataError) as e:
            logger.error(f"Failed to get offline features: {e}")
            return entity_df
    
    def _get_historical_feature(self, feature_name: str, entity_key: str, 
                               entity_value: str, event_timestamp: datetime) -> Any:
        """Get historical feature value with point-in-time correctness"""
        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT feature_value
                        FROM feature_values
                        WHERE feature_name = %s 
                        AND entity_key = %s 
                        AND entity_value = %s
                        AND event_timestamp <= %s
                        ORDER BY event_timestamp DESC
                        LIMIT 1
                    """, (feature_name, entity_key, entity_value, event_timestamp))
                    
                    result = cur.fetchone()
                    return result['feature_value'] if result else None
                    
        except (psycopg2.Error, KeyError) as e:
            logger.error(f"Historical feature lookup failed: {e}")
            return None
    
    def _generate_cache_key(self, feature_names: List[str], entity_key: str, entity_value: str) -> str:
        """Generate cache key for feature combination"""
        key_string = f"{sorted(feature_names)}:{entity_key}:{entity_value}"
        return f"features:{hashlib.md5(key_string.encode()).hexdigest()}"
    
    def get_performance_metrics(self) -> Dict[str, float]:
        """Get feature store performance metrics"""
        if not self.serving_latency:
            return {}
        
        return {
            'avg_latency_ms': np.mean(self.serving_latency[-1000:]),  # Last 1000 requests
            'p95_latency_ms': np.percentile(self.serving_latency[-1000:], 95),
            'p99_latency_ms': np.percentile(self.serving_latency[-1000:], 99),
            'cache_hit_ratio': np.mean(self.cache_hit_ratio[-1000:]),
            'total_requests': len(self.serving_latency)
        }
    
    def health_check(self) -> Dict[str, bool]:
        """Check health of feature store components"""
        health = {}
        
        try:
            self.redis_client.ping()
            health['redis'] = True
        except (redis.RedisError, ConnectionError):
            health['redis'] = False
        
        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            health['postgres'] = True
        except (psycopg2.Error, ConnectionError):
            health['postgres'] = False
        
        return health
