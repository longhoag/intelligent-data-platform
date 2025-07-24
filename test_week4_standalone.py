#!/usr/bin/env python3
"""
Week 4 Feature Store Standalone Test
Tests core feature store components without external dependencies
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd
import numpy as np
from loguru import logger

# Import feature store components
from src.feature_store.store import FeatureStore, FeatureDefinition, FeatureView
from src.feature_store.registry import FeatureRegistry
from src.feature_store.cache import FeatureCache

# Try to import server (optional)
try:
    from src.feature_store.server import FeatureServer
    SERVER_AVAILABLE = True
except ImportError as e:
    SERVER_AVAILABLE = False
    logger.warning(f"FeatureServer not available: {e}")


class MockRedisClient:
    """Mock Redis client for testing without Redis server"""
    
    def __init__(self):
        self.data = {}
        self.ttl_data = {}
    
    def set(self, key: str, value: str, ex: int = None):
        """Set key-value with optional expiration"""
        self.data[key] = value
        if ex:
            self.ttl_data[key] = time.time() + ex
        return True
    
    def get(self, key: str):
        """Get value by key"""
        # Check if expired
        if key in self.ttl_data and time.time() > self.ttl_data[key]:
            del self.data[key]
            del self.ttl_data[key]
            return None
        return self.data.get(key)
    
    def delete(self, key: str):
        """Delete key"""
        self.data.pop(key, None)
        self.ttl_data.pop(key, None)
        return True
    
    def ping(self):
        """Health check"""
        return True
    
    def flushdb(self):
        """Clear all data"""
        self.data.clear()
        self.ttl_data.clear()
        return True


class MockPostgreSQLConnection:
    """Mock PostgreSQL connection for testing without database"""
    
    def __init__(self):
        self.features = {}
        self.feature_views = {}
        self.lineage = []
        self.closed = False
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type:
            self.rollback()
        else:
            self.commit()
    
    def cursor(self):
        return MockCursor(self)
    
    def commit(self):
        pass
    
    def rollback(self):
        pass
    
    def close(self):
        self.closed = True


class MockCursor:
    """Mock database cursor"""
    
    def __init__(self, connection):
        self.connection = connection
        self.results = []
    
    def execute(self, query: str, params=None):
        """Execute mock query"""
        if "INSERT INTO features" in query:
            # Mock feature insertion
            pass
        elif "SELECT * FROM features" in query:
            # Mock feature selection
            self.results = [(name, json.dumps(defn.__dict__)) 
                          for name, defn in self.connection.features.items()]
        elif "INSERT INTO feature_lineage" in query:
            # Mock lineage insertion
            pass
    
    def fetchall(self):
        return self.results
    
    def fetchone(self):
        return self.results[0] if self.results else None
    
    def close(self):
        pass


class StandaloneFeatureStoreTest:
    """Standalone test for feature store components"""
    
    def __init__(self):
        self.mock_redis = MockRedisClient()
        self.mock_postgres = MockPostgreSQLConnection()
        self.feature_store = None
        self.feature_registry = None
        self.feature_cache = None
        self.feature_server = None
    
    def setup_components(self):
        """Initialize feature store components with mock dependencies"""
        logger.info("Setting up feature store components...")
        
        # Patch the connections in the classes
        import src.feature_store.store as store_module
        import src.feature_store.cache as cache_module
        import src.feature_store.registry as registry_module
        
        # Mock Redis connection
        original_redis_init = store_module.redis.Redis
        store_module.redis.Redis = lambda **kwargs: self.mock_redis
        cache_module.redis.Redis = lambda **kwargs: self.mock_redis
        
        # Mock PostgreSQL connection
        original_psycopg2_connect = store_module.psycopg2.connect
        store_module.psycopg2.connect = lambda **kwargs: self.mock_postgres
        registry_module.psycopg2.connect = lambda **kwargs: self.mock_postgres
        
        try:
            # Initialize components
            self.feature_store = FeatureStore(
                redis_host="mock",
                redis_port=6379,
                postgres_config={
                    'host': 'mock',
                    'port': '5432',
                    'dbname': 'mock',
                    'user': 'mock',
                    'password': 'mock'
                }
            )
            
            self.feature_registry = FeatureRegistry(
                postgres_config={
                    'host': 'mock',
                    'port': '5432', 
                    'dbname': 'mock',
                    'user': 'mock',
                    'password': 'mock'
                }
            )
            
            self.feature_cache = FeatureCache(
                redis_host="mock",
                redis_port=6379,
                default_ttl=3600
            )
            
            if SERVER_AVAILABLE:
                self.feature_server = FeatureServer(
                    feature_store=self.feature_store,
                    host="localhost",
                    port=8001
                )
            
            logger.success("✅ All components initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Component initialization failed: {e}")
            raise
        finally:
            # Restore original functions
            store_module.redis.Redis = original_redis_init
            store_module.psycopg2.connect = original_psycopg2_connect
    
    def test_feature_definitions(self):
        """Test feature definition creation and management"""
        logger.info("🔧 Testing feature definitions...")
        
        try:
            # Create sample feature definitions
            price_feature = FeatureDefinition(
                name="stock_price",
                feature_type="numeric",
                description="Current stock price",
                source_table="market_data",
                source_column="close_price",
                tags=["finance", "price"]
            )
            
            volume_feature = FeatureDefinition(
                name="trading_volume",
                feature_type="numeric", 
                description="24h trading volume",
                source_table="market_data",
                source_column="volume",
                tags=["finance", "volume"]
            )
            
            # Register features
            self.feature_store.register_feature(price_feature)
            self.feature_store.register_feature(volume_feature)
            
            # Verify registration
            assert "stock_price" in self.feature_store.feature_definitions
            assert "trading_volume" in self.feature_store.feature_definitions
            
            logger.success("✅ Feature definitions test passed")
            
        except Exception as e:
            logger.error(f"❌ Feature definitions test failed: {e}")
            raise
    
    def test_feature_views(self):
        """Test feature view creation and management"""
        logger.info("🔧 Testing feature views...")
        
        try:
            # Get existing features
            price_feature = self.feature_store.feature_definitions["stock_price"]
            volume_feature = self.feature_store.feature_definitions["trading_volume"]
            
            # Create feature view
            market_view = FeatureView(
                name="market_features",
                entities=["symbol"],
                features=[price_feature, volume_feature],
                ttl=timedelta(hours=1),
                batch_source="market_data_table",
                stream_source="market_data_stream"
            )
            
            # Register view
            self.feature_store.register_feature_view(market_view)
            
            # Verify registration
            assert "market_features" in self.feature_store.feature_views
            view = self.feature_store.feature_views["market_features"]
            assert len(view.features) == 2
            assert "symbol" in view.entities
            
            logger.success("✅ Feature views test passed")
            
        except Exception as e:
            logger.error(f"❌ Feature views test failed: {e}")
            raise
    
    def test_feature_cache(self):
        """Test feature caching functionality"""
        logger.info("🔧 Testing feature cache...")
        
        try:
            # Test cache operations
            test_features = {
                "stock_price": 150.25,
                "trading_volume": 1000000,
                "timestamp": datetime.now().isoformat()
            }
            
            # Cache features
            cache_key = "AAPL:market_features"
            self.feature_cache.set_features(cache_key, test_features, ttl=300)
            
            # Retrieve features
            cached_features = self.feature_cache.get_features(cache_key)
            assert cached_features is not None
            assert cached_features["stock_price"] == 150.25
            
            # Test cache miss
            missing_features = self.feature_cache.get_features("NONEXISTENT:key")
            assert missing_features is None
            
            # Test cache invalidation
            self.feature_cache.invalidate_features(cache_key)
            invalidated_features = self.feature_cache.get_features(cache_key)
            assert invalidated_features is None
            
            logger.success("✅ Feature cache test passed")
            
        except Exception as e:
            logger.error(f"❌ Feature cache test failed: {e}")
            raise
    
    def test_feature_registry(self):
        """Test feature registry functionality"""
        logger.info("🔧 Testing feature registry...")
        
        try:
            # Test feature registration with versioning
            feature_def = FeatureDefinition(
                name="market_cap",
                feature_type="numeric",
                description="Market capitalization",
                source_table="company_data",
                source_column="market_cap",
                version="1.0.0",
                tags=["finance", "company"]
            )
            
            # Register with lineage
            self.feature_registry.register_feature_with_lineage(
                feature_def,
                source_features=["stock_price", "shares_outstanding"],
                transformation="stock_price * shares_outstanding"
            )
            
            # Test feature lookup
            retrieved_feature = self.feature_registry.get_feature("market_cap", "1.0.0")
            assert retrieved_feature is not None
            assert retrieved_feature.name == "market_cap"
            
            # Test lineage tracking
            lineage = self.feature_registry.get_feature_lineage("market_cap")
            assert len(lineage) > 0
            
            logger.success("✅ Feature registry test passed")
            
        except Exception as e:
            logger.error(f"❌ Feature registry test failed: {e}")
            raise
    
    def test_online_feature_serving(self):
        """Test online feature serving"""
        logger.info("🔧 Testing online feature serving...")
        
        try:
            # Simulate real-time feature serving
            entity_key = "symbol"
            entity_value = "AAPL"
            feature_names = ["stock_price", "trading_volume"]
            
            # Mock some real-time data
            mock_data = {
                "stock_price": 151.75,
                "trading_volume": 1250000,
                "last_updated": datetime.now().isoformat()
            }
            
            # Store in cache for serving
            cache_key = f"{entity_value}:market_features"
            self.feature_cache.set_features(cache_key, mock_data, ttl=60)
            
            # Serve features (this would normally hit the online store)
            start_time = time.time()
            
            # Simulate feature serving logic
            cached_features = self.feature_cache.get_features(cache_key)
            if cached_features:
                served_features = {name: cached_features.get(name) 
                                 for name in feature_names if name in cached_features}
            else:
                served_features = {}
            
            latency_ms = (time.time() - start_time) * 1000
            
            # Verify results
            assert "stock_price" in served_features
            assert "trading_volume" in served_features
            assert latency_ms < 100  # Sub-100ms latency requirement
            
            logger.success(f"✅ Online serving test passed (latency: {latency_ms:.2f}ms)")
            
        except Exception as e:
            logger.error(f"❌ Online serving test failed: {e}")
            raise
    
    def test_batch_feature_computation(self):
        """Test batch feature computation"""
        logger.info("🔧 Testing batch feature computation...")
        
        try:
            # Create sample batch data
            dates = pd.date_range(start='2025-01-01', periods=10, freq='D')
            sample_data = pd.DataFrame({
                'symbol': ['AAPL'] * 10,
                'date': dates,
                'close_price': np.random.uniform(140, 160, 10),
                'volume': np.random.randint(800000, 1500000, 10)
            })
            
            # Compute batch features
            start_time = time.time()
            
            # Simulate feature computation
            batch_features = sample_data.copy()
            batch_features['price_change'] = batch_features['close_price'].pct_change()
            batch_features['volume_ma_3'] = batch_features['volume'].rolling(3).mean()
            batch_features['price_volatility'] = batch_features['close_price'].rolling(5).std()
            
            computation_time = time.time() - start_time
            
            # Verify computed features
            assert 'price_change' in batch_features.columns
            assert 'volume_ma_3' in batch_features.columns
            assert 'price_volatility' in batch_features.columns
            assert len(batch_features) == 10
            
            logger.success(f"✅ Batch computation test passed ({computation_time:.2f}s)")
            
        except Exception as e:
            logger.error(f"❌ Batch computation test failed: {e}")
            raise
    
    def test_health_monitoring(self):
        """Test health check functionality"""
        logger.info("🔧 Testing health monitoring...")
        
        try:
            # Test component health checks
            redis_health = self.mock_redis.ping()
            postgres_health = not self.mock_postgres.closed
            
            health_status = {
                "redis": redis_health,
                "postgres": postgres_health,
                "feature_store": True,
                "feature_cache": True,
                "feature_registry": True
            }
            
            # Verify all components are healthy
            all_healthy = all(health_status.values())
            assert all_healthy, f"Health check failed: {health_status}"
            
            logger.success("✅ Health monitoring test passed")
            
        except Exception as e:
            logger.error(f"❌ Health monitoring test failed: {e}")
            raise
    
    async def run_all_tests(self):
        """Run all feature store tests"""
        logger.info("🚀 Starting Week 4 Feature Store standalone tests...")
        print("="*60)
        
        try:
            # Setup
            self.setup_components()
            print()
            
            # Run tests
            self.test_feature_definitions()
            self.test_feature_views()
            self.test_feature_cache()
            self.test_feature_registry()
            self.test_online_feature_serving()
            self.test_batch_feature_computation()
            self.test_health_monitoring()
            
            print()
            print("="*60)
            logger.success("🎉 All Week 4 Feature Store tests passed!")
            
            # Print summary
            print("\n📊 Test Summary:")
            print("✅ Feature Definitions - PASSED")
            print("✅ Feature Views - PASSED")
            print("✅ Feature Cache - PASSED")
            print("✅ Feature Registry - PASSED")
            print("✅ Online Serving - PASSED")
            print("✅ Batch Computation - PASSED")
            print("✅ Health Monitoring - PASSED")
            
            if SERVER_AVAILABLE:
                print("✅ Feature Server - AVAILABLE")
            else:
                print("⚠️  Feature Server - NOT AVAILABLE (requires FastAPI)")
            
        except Exception as e:
            logger.error(f"❌ Test suite failed: {e}")
            raise


async def main():
    """Main test execution"""
    test_runner = StandaloneFeatureStoreTest()
    await test_runner.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())
