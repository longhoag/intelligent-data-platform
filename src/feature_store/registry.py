"""
Feature Registry - Day 4
Feature versioning, lineage tracking, and discovery
"""

import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from loguru import logger
import psycopg2  # type: ignore
from psycopg2.extras import RealDictCursor  # type: ignore


@dataclass
class FeatureVersion:
    """Feature version metadata"""
    feature_name: str
    version: str
    schema_hash: str
    created_at: datetime
    created_by: str
    description: str
    status: str  # 'active', 'deprecated', 'retired'
    parent_version: Optional[str] = None
    rollback_version: Optional[str] = None


@dataclass 
class FeatureLineage:
    """Feature lineage tracking"""
    feature_name: str
    version: str
    source_features: List[str]
    transformation_code: str
    data_sources: List[str]
    created_at: datetime


class FeatureRegistry:
    """Feature versioning and lineage management"""
    
    def __init__(self, postgres_config: Dict[str, str]):
        self.postgres_config = postgres_config
        self._initialize_registry_tables()
        logger.info("Feature registry initialized")
    
    def _initialize_registry_tables(self):
        """Initialize registry tables"""
        with psycopg2.connect(**self.postgres_config) as conn:
            with conn.cursor() as cur:
                # Feature versions table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS feature_versions (
                        feature_name VARCHAR(255),
                        version VARCHAR(50),
                        schema_hash VARCHAR(64),
                        created_at TIMESTAMP,
                        created_by VARCHAR(255),
                        description TEXT,
                        status VARCHAR(50),
                        parent_version VARCHAR(50),
                        rollback_version VARCHAR(50),
                        metadata JSONB,
                        PRIMARY KEY (feature_name, version)
                    )
                """)
                
                # Feature lineage table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS feature_lineage (
                        feature_name VARCHAR(255),
                        version VARCHAR(50),
                        source_features TEXT[],
                        transformation_code TEXT,
                        data_sources TEXT[],
                        created_at TIMESTAMP,
                        lineage_hash VARCHAR(64),
                        PRIMARY KEY (feature_name, version)
                    )
                """)
                
                # Feature usage tracking
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS feature_usage (
                        feature_name VARCHAR(255),
                        version VARCHAR(50),
                        model_name VARCHAR(255),
                        environment VARCHAR(100),
                        usage_count INTEGER DEFAULT 0,
                        last_used TIMESTAMP,
                        created_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (feature_name, version, model_name, environment)
                    )
                """)
                
                # Feature performance tracking
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS feature_performance (
                        feature_name VARCHAR(255),
                        version VARCHAR(50),
                        metric_name VARCHAR(100),
                        metric_value FLOAT,
                        measurement_time TIMESTAMP,
                        environment VARCHAR(100)
                    )
                """)
                
                conn.commit()
                logger.info("Registry tables initialized")
    
    def create_feature_version(self, 
                             feature_name: str,
                             version: str,
                             schema: Dict[str, Any],
                             created_by: str,
                             description: str,
                             parent_version: Optional[str] = None) -> FeatureVersion:
        """Create a new feature version"""
        try:
            # Generate schema hash
            schema_str = json.dumps(schema, sort_keys=True)
            schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()
            
            # Create version object
            feature_version = FeatureVersion(
                feature_name=feature_name,
                version=version,
                schema_hash=schema_hash,
                created_at=datetime.now(),
                created_by=created_by,
                description=description,
                status='active',
                parent_version=parent_version
            )
            
            # Store in database
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO feature_versions 
                        (feature_name, version, schema_hash, created_at, created_by, 
                         description, status, parent_version, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        feature_name, version, schema_hash, feature_version.created_at,
                        created_by, description, 'active', parent_version,
                        json.dumps(schema)
                    ))
                    conn.commit()
            
            logger.info(f"Feature version created: {feature_name} v{version}")
            return feature_version
            
        except Exception as e:
            logger.error(f"Failed to create feature version: {e}")
            raise
    
    def get_feature_version(self, feature_name: str, version: str) -> Optional[FeatureVersion]:
        """Get specific feature version"""
        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM feature_versions
                        WHERE feature_name = %s AND version = %s
                    """, (feature_name, version))
                    
                    result = cur.fetchone()
                    if result:
                        return FeatureVersion(**dict(result))
                    return None
                    
        except (psycopg2.Error, TypeError) as e:
            logger.error(f"Failed to get feature version: {e}")
            return None
    
    def list_feature_versions(self, feature_name: str) -> List[FeatureVersion]:
        """List all versions of a feature"""
        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM feature_versions
                        WHERE feature_name = %s
                        ORDER BY created_at DESC
                    """, (feature_name,))
                    
                    results = cur.fetchall()
                    return [FeatureVersion(**dict(r)) for r in results]
                    
        except (psycopg2.Error, TypeError) as e:
            logger.error(f"Failed to list feature versions: {e}")
            return []
    
    def get_latest_version(self, feature_name: str) -> Optional[FeatureVersion]:
        """Get latest active version of a feature"""
        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM feature_versions
                        WHERE feature_name = %s AND status = 'active'
                        ORDER BY created_at DESC
                        LIMIT 1
                    """, (feature_name,))
                    
                    result = cur.fetchone()
                    if result:
                        return FeatureVersion(**dict(result))
                    return None
                    
        except Exception as e:
            logger.error(f"Failed to get latest version: {e}")
            return None
    
    def deprecate_version(self, feature_name: str, version: str, rollback_version: str = None):
        """Deprecate a feature version"""
        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE feature_versions
                        SET status = 'deprecated', rollback_version = %s
                        WHERE feature_name = %s AND version = %s
                    """, (rollback_version, feature_name, version))
                    conn.commit()
            
            logger.info(f"Feature version deprecated: {feature_name} v{version}")
            
        except Exception as e:
            logger.error(f"Failed to deprecate version: {e}")
            raise
    
    def rollback_feature(self, feature_name: str, target_version: str):
        """Rollback feature to a previous version"""
        try:
            # Get current active version
            current_version = self.get_latest_version(feature_name)
            if not current_version:
                raise ValueError("No active version found")
            
            # Get target version
            target = self.get_feature_version(feature_name, target_version)
            if not target:
                raise ValueError(f"Target version {target_version} not found")
            
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    # Deprecate current version
                    cur.execute("""
                        UPDATE feature_versions
                        SET status = 'deprecated', rollback_version = %s
                        WHERE feature_name = %s AND version = %s
                    """, (target_version, feature_name, current_version.version))
                    
                    # Activate target version
                    cur.execute("""
                        UPDATE feature_versions
                        SET status = 'active'
                        WHERE feature_name = %s AND version = %s
                    """, (feature_name, target_version))
                    
                    conn.commit()
            
            logger.info(f"Feature rolled back: {feature_name} from v{current_version.version} to v{target_version}")
            
        except Exception as e:
            logger.error(f"Failed to rollback feature: {e}")
            raise
    
    def track_lineage(self, 
                     feature_name: str,
                     version: str,
                     source_features: List[str],
                     transformation_code: str,
                     data_sources: List[str]):
        """Track feature lineage"""
        try:
            # Generate lineage hash
            lineage_str = json.dumps({
                'sources': sorted(source_features),
                'transformation': transformation_code,
                'data_sources': sorted(data_sources)
            }, sort_keys=True)
            lineage_hash = hashlib.sha256(lineage_str.encode()).hexdigest()
            
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO feature_lineage
                        (feature_name, version, source_features, transformation_code,
                         data_sources, created_at, lineage_hash)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (feature_name, version) DO UPDATE SET
                        source_features = EXCLUDED.source_features,
                        transformation_code = EXCLUDED.transformation_code,
                        data_sources = EXCLUDED.data_sources,
                        lineage_hash = EXCLUDED.lineage_hash
                    """, (
                        feature_name, version, source_features, transformation_code,
                        data_sources, datetime.now(), lineage_hash
                    ))
                    conn.commit()
            
            logger.info(f"Lineage tracked for {feature_name} v{version}")
            
        except Exception as e:
            logger.error(f"Failed to track lineage: {e}")
            raise
    
    def get_feature_lineage(self, feature_name: str, version: str) -> Optional[FeatureLineage]:
        """Get feature lineage"""
        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM feature_lineage
                        WHERE feature_name = %s AND version = %s
                    """, (feature_name, version))
                    
                    result = cur.fetchone()
                    if result:
                        return FeatureLineage(**dict(result))
                    return None
                    
        except Exception as e:
            logger.error(f"Failed to get lineage: {e}")
            return None
    
    def track_feature_usage(self, 
                           feature_name: str,
                           version: str,
                           model_name: str,
                           environment: str = 'production'):
        """Track feature usage by models"""
        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO feature_usage
                        (feature_name, version, model_name, environment, usage_count, last_used)
                        VALUES (%s, %s, %s, %s, 1, %s)
                        ON CONFLICT (feature_name, version, model_name, environment) DO UPDATE SET
                        usage_count = feature_usage.usage_count + 1,
                        last_used = EXCLUDED.last_used
                    """, (feature_name, version, model_name, environment, datetime.now()))
                    conn.commit()
                    
        except Exception as e:
            logger.error(f"Failed to track usage: {e}")
    
    def get_feature_usage(self, feature_name: str, version: str = None) -> List[Dict[str, Any]]:
        """Get feature usage statistics"""
        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    if version:
                        cur.execute("""
                            SELECT * FROM feature_usage
                            WHERE feature_name = %s AND version = %s
                            ORDER BY usage_count DESC
                        """, (feature_name, version))
                    else:
                        cur.execute("""
                            SELECT * FROM feature_usage
                            WHERE feature_name = %s
                            ORDER BY usage_count DESC
                        """, (feature_name,))
                    
                    return [dict(r) for r in cur.fetchall()]
                    
        except Exception as e:
            logger.error(f"Failed to get usage: {e}")
            return []
    
    def discover_features(self, 
                         tag: Optional[str] = None,
                         source_table: Optional[str] = None,
                         feature_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover features by criteria"""
        try:
            # This would typically query the main feature store
            # For now, return a placeholder implementation
            filters = []
            params = []
            
            query = "SELECT DISTINCT feature_name, schema FROM feature_versions WHERE 1=1"
            
            if tag:
                filters.append("schema::text LIKE %s")
                params.append(f'%"tags":[%{tag}%]%')
            
            if source_table:
                filters.append("schema::text LIKE %s")
                params.append(f'%"source_table":"{source_table}"%')
                
            if feature_type:
                filters.append("schema::text LIKE %s")
                params.append(f'%"feature_type":"{feature_type}"%')
            
            if filters:
                query += " AND " + " AND ".join(filters)
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    results = cur.fetchall()
                    
                    features = []
                    for result in results:
                        features.append({
                            'name': result['feature_name'],
                            'schema': result['schema']
                        })
            
            return features
            
        except (psycopg2.Error, json.JSONDecodeError) as e:
            logger.error(f"Failed to discover features: {e}")
            return []
    
    def record_performance_metric(self,
                                feature_name: str,
                                version: str,
                                metric_name: str,
                                metric_value: float,
                                environment: str = 'production'):
        """Record feature performance metric"""
        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO feature_performance
                        (feature_name, version, metric_name, metric_value, measurement_time, environment)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (feature_name, version, metric_name, metric_value, 
                          datetime.now(), environment))
                    conn.commit()
                    
        except (psycopg2.Error, ValueError) as e:
            logger.error(f"Failed to record performance metric: {e}")
    
    def get_performance_metrics(self,
                              feature_name: str,
                              version: Optional[str] = None,
                              days: int = 7) -> List[Dict[str, Any]]:
        """Get feature performance metrics"""
        try:
            since_date = datetime.now() - timedelta(days=days)
            
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    if version:
                        cur.execute("""
                            SELECT * FROM feature_performance
                            WHERE feature_name = %s AND version = %s 
                            AND measurement_time >= %s
                            ORDER BY measurement_time DESC
                        """, (feature_name, version, since_date))
                    else:
                        cur.execute("""
                            SELECT * FROM feature_performance
                            WHERE feature_name = %s AND measurement_time >= %s
                            ORDER BY measurement_time DESC
                        """, (feature_name, since_date))
                    
                    return [dict(r) for r in cur.fetchall()]
                    
        except Exception as e:
            logger.error(f"Failed to get performance metrics: {e}")
            return []
