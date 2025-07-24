"""
Feature Cache - Day 4
High-performance caching strategies for feature serving
"""

import time
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from loguru import logger
import redis  # type: ignore
from dataclasses import dataclass


@dataclass
class CacheStats:
    """Cache performance statistics"""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    total_requests: int = 0
    avg_latency_ms: float = 0.0
    
    @property
    def hit_ratio(self) -> float:
        return self.hits / max(self.total_requests, 1)


class FeatureCache:
    """High-performance feature caching with multiple strategies"""
    
    def __init__(self, 
                 redis_host: str = 'localhost',
                 redis_port: int = 6379,
                 default_ttl: int = 3600):  # 1 hour default TTL
        
        # Multiple Redis connections for different cache tiers
        self.redis_hot = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            db=0,  # Hot cache - frequently accessed features
            decode_responses=True,
            socket_keepalive=True,
            socket_keepalive_options={},
            health_check_interval=30
        )
        
        self.redis_warm = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            db=1,  # Warm cache - less frequently accessed
            decode_responses=True,
            socket_keepalive=True,
            socket_keepalive_options={},
            health_check_interval=30
        )
        
        self.default_ttl = default_ttl
        self.stats = CacheStats()
        self.latency_samples = []
        
        # Cache tier thresholds
        self.hot_access_threshold = 10  # Access count to promote to hot tier
        self.hot_ttl = 3600  # 1 hour for hot features
        self.warm_ttl = 86400  # 24 hours for warm features
        
        logger.info("Feature cache initialized with tiered storage")
    
    async def get(self, key: str) -> Optional[Any]:
        """Get feature from cache with tier promotion"""
        start_time = time.time()
        
        try:
            # Check hot cache first
            value = self.redis_hot.get(key)
            if value:
                self.stats.hits += 1
                self._record_latency(start_time)
                logger.debug(f"Hot cache hit: {key}")
                return json.loads(value)
            
            # Check warm cache
            value = self.redis_warm.get(key)
            if value:
                self.stats.hits += 1
                self._record_latency(start_time)
                
                # Promote to hot cache if accessed frequently
                access_count = await self._get_access_count(key)
                if access_count >= self.hot_access_threshold:
                    await self._promote_to_hot(key, value)
                    logger.debug(f"Promoted to hot cache: {key}")
                
                logger.debug(f"Warm cache hit: {key}")
                return json.loads(value)
            
            # Cache miss
            self.stats.misses += 1
            self._record_latency(start_time)
            logger.debug(f"Cache miss: {key}")
            return None
            
        except (redis.RedisError, json.JSONDecodeError) as e:
            logger.error(f"Cache get error: {e}")
            self.stats.misses += 1
            return None
        finally:
            self.stats.total_requests += 1
    
    async def set(self, 
                  key: str, 
                  value: Any, 
                  ttl: Optional[int] = None,
                  tier: str = 'warm') -> bool:
        """Set feature in cache with appropriate tier"""
        try:
            if ttl is None:
                ttl = self.default_ttl
            
            serialized_value = json.dumps(value)
            
            if tier == 'hot':
                # Store in hot cache
                self.redis_hot.setex(key, self.hot_ttl, serialized_value)
                logger.debug(f"Cached in hot tier: {key}")
            else:
                # Store in warm cache
                self.redis_warm.setex(key, self.warm_ttl, serialized_value)
                logger.debug(f"Cached in warm tier: {key}")
            
            # Track access count
            await self._increment_access_count(key)
            
            return True
            
        except (redis.RedisError, ValueError) as e:
            logger.error(f"Cache set error: {e}")
            return False
    
    async def get_batch(self, keys: List[str]) -> Dict[str, Any]:
        """Get multiple features from cache efficiently"""
        start_time = time.time()
        results = {}
        
        try:
            # Batch get from hot cache
            hot_pipe = self.redis_hot.pipeline()
            for key in keys:
                hot_pipe.get(key)
            hot_results = hot_pipe.execute()
            
            # Collect hot cache hits
            missing_keys = []
            for key, value in zip(keys, hot_results):
                if value:
                    results[key] = json.loads(value)
                    self.stats.hits += 1
                else:
                    missing_keys.append(key)
            
            # Batch get from warm cache for missing keys
            if missing_keys:
                warm_pipe = self.redis_warm.pipeline()
                for key in missing_keys:
                    warm_pipe.get(key)
                warm_results = warm_pipe.execute()
                
                # Collect warm cache hits and promote if needed
                for key, value in zip(missing_keys, warm_results):
                    if value:
                        results[key] = json.loads(value)
                        self.stats.hits += 1
                        
                        # Check for promotion
                        access_count = await self._get_access_count(key)
                        if access_count >= self.hot_access_threshold:
                            await self._promote_to_hot(key, value)
                    else:
                        self.stats.misses += 1
            
            self._record_latency(start_time)
            self.stats.total_requests += len(keys)
            
            return results
            
        except (redis.RedisError, json.JSONDecodeError) as e:
            logger.error(f"Batch cache get error: {e}")
            self.stats.misses += len(keys)
            self.stats.total_requests += len(keys)
            return {}
    
    async def set_batch(self, 
                       items: Dict[str, Any], 
                       ttl: Optional[int] = None,
                       tier: str = 'warm') -> bool:
        """Set multiple features in cache efficiently"""
        try:
            if ttl is None:
                ttl = self.default_ttl
            
            # Choose cache tier
            cache_client = self.redis_hot if tier == 'hot' else self.redis_warm
            cache_ttl = self.hot_ttl if tier == 'hot' else self.warm_ttl
            
            # Batch set
            pipe = cache_client.pipeline()
            for key, value in items.items():
                serialized_value = json.dumps(value)
                pipe.setex(key, cache_ttl, serialized_value)
            
            pipe.execute()
            
            # Update access counts
            access_pipe = self.redis_warm.pipeline()
            for key in items.keys():
                access_count_key = f"access_count:{key}"
                access_pipe.incr(access_count_key)
                access_pipe.expire(access_count_key, 86400)  # Expire access counts after 24h
            access_pipe.execute()
            
            logger.debug(f"Batch cached {len(items)} items in {tier} tier")
            return True
            
        except (redis.RedisError, ValueError) as e:
            logger.error(f"Batch cache set error: {e}")
            return False
    
    async def invalidate(self, key: str) -> bool:
        """Invalidate feature from all cache tiers"""
        try:
            # Remove from both tiers
            hot_deleted = self.redis_hot.delete(key)
            warm_deleted = self.redis_warm.delete(key)
            
            # Clear access count
            access_count_key = f"access_count:{key}"
            self.redis_warm.delete(access_count_key)
            
            if hot_deleted or warm_deleted:
                logger.debug(f"Invalidated cache key: {key}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Cache invalidation error: {e}")
            return False
    
    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching pattern"""
        try:
            deleted_count = 0
            
            # Find and delete from hot cache
            hot_keys = self.redis_hot.keys(pattern)
            if hot_keys:
                deleted_count += self.redis_hot.delete(*hot_keys)
            
            # Find and delete from warm cache
            warm_keys = self.redis_warm.keys(pattern)
            if warm_keys:
                deleted_count += self.redis_warm.delete(*warm_keys)
            
            logger.info(f"Invalidated {deleted_count} keys matching pattern: {pattern}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Pattern invalidation error: {e}")
            return 0
    
    async def _get_access_count(self, key: str) -> int:
        """Get access count for a key"""
        try:
            access_count_key = f"access_count:{key}"
            count = self.redis_warm.get(access_count_key)
            return int(count) if count else 0
        except:
            return 0
    
    async def _increment_access_count(self, key: str):
        """Increment access count for a key"""
        try:
            access_count_key = f"access_count:{key}"
            self.redis_warm.incr(access_count_key)
            self.redis_warm.expire(access_count_key, 86400)  # 24 hour expiry
        except Exception as e:
            logger.debug(f"Access count increment failed: {e}")
    
    async def _promote_to_hot(self, key: str, value: str):
        """Promote feature from warm to hot cache"""
        try:
            self.redis_hot.setex(key, self.hot_ttl, value)
            # Keep in warm cache as well for redundancy
        except Exception as e:
            logger.error(f"Cache promotion error: {e}")
    
    def _record_latency(self, start_time: float):
        """Record cache operation latency"""
        latency_ms = (time.time() - start_time) * 1000
        self.latency_samples.append(latency_ms)
        
        # Keep only recent samples
        if len(self.latency_samples) > 1000:
            self.latency_samples = self.latency_samples[-1000:]
    
    def warm_cache(self, 
                   feature_data: Dict[str, Any],
                   entities: List[Dict[str, str]]):
        """Pre-warm cache with feature data"""
        try:
            cache_items = {}
            
            for entity in entities:
                for entity_key, entity_value in entity.items():
                    for feature_name, feature_value in feature_data.items():
                        cache_key = f"feature:{feature_name}:{entity_key}:{entity_value}"
                        cache_items[cache_key] = feature_value
            
            # Batch load into warm cache
            if cache_items:
                pipe = self.redis_warm.pipeline()
                for key, value in cache_items.items():
                    serialized_value = json.dumps(value)
                    pipe.setex(key, self.warm_ttl, serialized_value)
                pipe.execute()
                
                logger.info(f"Pre-warmed cache with {len(cache_items)} feature values")
            
        except Exception as e:
            logger.error(f"Cache warming error: {e}")
    
    def get_cache_stats(self) -> CacheStats:
        """Get cache performance statistics"""
        if self.latency_samples:
            self.stats.avg_latency_ms = sum(self.latency_samples) / len(self.latency_samples)
        
        return self.stats
    
    def get_memory_usage(self) -> Dict[str, Any]:
        """Get cache memory usage statistics"""
        try:
            hot_info = self.redis_hot.info('memory')
            warm_info = self.redis_warm.info('memory')
            
            return {
                'hot_cache': {
                    'used_memory': hot_info.get('used_memory', 0),
                    'used_memory_human': hot_info.get('used_memory_human', '0B'),
                    'maxmemory': hot_info.get('maxmemory', 0)
                },
                'warm_cache': {
                    'used_memory': warm_info.get('used_memory', 0),
                    'used_memory_human': warm_info.get('used_memory_human', '0B'),
                    'maxmemory': warm_info.get('maxmemory', 0)
                }
            }
            
        except Exception as e:
            logger.error(f"Memory usage query error: {e}")
            return {}
    
    def clear_all_caches(self):
        """Clear all cache tiers (use with caution)"""
        try:
            self.redis_hot.flushdb()
            self.redis_warm.flushdb()
            logger.warning("All caches cleared")
        except Exception as e:
            logger.error(f"Cache clear error: {e}")
    
    def health_check(self) -> Dict[str, bool]:
        """Check health of cache components"""
        health = {}
        
        try:
            self.redis_hot.ping()
            health['hot_cache'] = True
        except:
            health['hot_cache'] = False
        
        try:
            self.redis_warm.ping()
            health['warm_cache'] = True
        except:
            health['warm_cache'] = False
        
        return health
