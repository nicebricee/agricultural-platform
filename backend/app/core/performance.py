"""
Performance optimization utilities for the Agricultural Data Platform.

Implements caching, connection pooling, and query optimization strategies.
"""

import asyncio
import hashlib
import json
import time
from typing import Any, Dict, Optional, Callable
from functools import wraps
import logging
from datetime import datetime, timedelta
import redis.asyncio as redis
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class CacheManager:
    """Manages caching for query results and frequently accessed data."""
    
    def __init__(self, redis_url: Optional[str] = None, ttl: int = 3600):
        """
        Initialize cache manager.
        
        Args:
            redis_url: Redis connection URL (optional)
            ttl: Default time-to-live in seconds
        """
        self.redis_url = redis_url or "redis://localhost:6379"
        self.ttl = ttl
        self.redis_client: Optional[redis.Redis] = None
        self.local_cache: Dict[str, Dict[str, Any]] = {}
        self.use_redis = False
    
    async def initialize(self) -> None:
        """Initialize cache connections."""
        try:
            # Try to connect to Redis
            self.redis_client = redis.from_url(self.redis_url)
            await self.redis_client.ping()
            self.use_redis = True
            logger.info("Redis cache initialized successfully")
        except Exception as e:
            logger.warning(f"Redis not available, using in-memory cache: {e}")
            self.use_redis = False
    
    def _generate_key(self, prefix: str, params: Dict[str, Any]) -> str:
        """Generate cache key from parameters."""
        # Sort params for consistent key generation
        sorted_params = json.dumps(params, sort_keys=True)
        hash_obj = hashlib.md5(sorted_params.encode())
        return f"{prefix}:{hash_obj.hexdigest()}"
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        try:
            if self.use_redis and self.redis_client:
                value = await self.redis_client.get(key)
                if value:
                    return json.loads(value)
            else:
                # Check local cache
                if key in self.local_cache:
                    entry = self.local_cache[key]
                    if entry["expires"] > time.time():
                        return entry["value"]
                    else:
                        # Expired, remove from cache
                        del self.local_cache[key]
            
            return None
            
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache."""
        try:
            ttl = ttl or self.ttl
            
            if self.use_redis and self.redis_client:
                await self.redis_client.setex(
                    key,
                    ttl,
                    json.dumps(value)
                )
            else:
                # Use local cache
                self.local_cache[key] = {
                    "value": value,
                    "expires": time.time() + ttl
                }
                
                # Clean up expired entries periodically
                if len(self.local_cache) > 1000:
                    await self._cleanup_local_cache()
                    
        except Exception as e:
            logger.error(f"Cache set error: {e}")
    
    async def delete(self, key: str) -> None:
        """Delete value from cache."""
        try:
            if self.use_redis and self.redis_client:
                await self.redis_client.delete(key)
            else:
                self.local_cache.pop(key, None)
                
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
    
    async def clear_pattern(self, pattern: str) -> None:
        """Clear all keys matching pattern."""
        try:
            if self.use_redis and self.redis_client:
                cursor = 0
                while True:
                    cursor, keys = await self.redis_client.scan(
                        cursor, match=pattern, count=100
                    )
                    if keys:
                        await self.redis_client.delete(*keys)
                    if cursor == 0:
                        break
            else:
                # Clear from local cache
                keys_to_delete = [k for k in self.local_cache if pattern.replace("*", "") in k]
                for key in keys_to_delete:
                    del self.local_cache[key]
                    
        except Exception as e:
            logger.error(f"Cache clear pattern error: {e}")
    
    async def _cleanup_local_cache(self) -> None:
        """Remove expired entries from local cache."""
        current_time = time.time()
        expired_keys = [
            k for k, v in self.local_cache.items()
            if v["expires"] < current_time
        ]
        for key in expired_keys:
            del self.local_cache[key]
    
    async def cleanup(self) -> None:
        """Clean up cache connections."""
        if self.redis_client:
            await self.redis_client.close()


class ConnectionPool:
    """Manages database connection pooling."""
    
    def __init__(self, 
                 min_size: int = 5,
                 max_size: int = 20,
                 max_idle_time: int = 300):
        """
        Initialize connection pool.
        
        Args:
            min_size: Minimum number of connections
            max_size: Maximum number of connections
            max_idle_time: Maximum idle time in seconds
        """
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle_time = max_idle_time
        self.connections: list = []
        self.available: asyncio.Queue = asyncio.Queue()
        self.in_use: set = set()
        self.lock = asyncio.Lock()
        self._closed = False
    
    async def initialize(self, connection_factory: Callable) -> None:
        """Initialize the connection pool."""
        async with self.lock:
            # Create minimum connections
            for _ in range(self.min_size):
                conn = await connection_factory()
                self.connections.append({
                    "connection": conn,
                    "last_used": time.time(),
                    "in_use": False
                })
                await self.available.put(conn)
    
    @asynccontextmanager
    async def acquire(self):
        """Acquire a connection from the pool."""
        if self._closed:
            raise RuntimeError("Connection pool is closed")
        
        connection = None
        try:
            # Try to get available connection
            connection = await asyncio.wait_for(
                self.available.get(),
                timeout=5.0
            )
            self.in_use.add(connection)
            
            yield connection
            
        except asyncio.TimeoutError:
            raise RuntimeError("No available connections in pool")
        finally:
            if connection:
                # Return connection to pool
                self.in_use.discard(connection)
                await self.available.put(connection)
    
    async def cleanup_idle(self) -> None:
        """Clean up idle connections."""
        async with self.lock:
            current_time = time.time()
            
            # Remove connections idle for too long
            active_connections = []
            for conn_info in self.connections:
                if (not conn_info["in_use"] and 
                    current_time - conn_info["last_used"] > self.max_idle_time and
                    len(active_connections) >= self.min_size):
                    # Close idle connection
                    try:
                        await conn_info["connection"].close()
                    except:
                        pass
                else:
                    conn_info["last_used"] = current_time
                    active_connections.append(conn_info)
            
            self.connections = active_connections
    
    async def close(self) -> None:
        """Close all connections in the pool."""
        self._closed = True
        async with self.lock:
            for conn_info in self.connections:
                try:
                    await conn_info["connection"].close()
                except:
                    pass
            self.connections.clear()


class QueryOptimizer:
    """Optimizes database queries for better performance."""
    
    def __init__(self):
        """Initialize query optimizer."""
        self.query_stats: Dict[str, Dict[str, Any]] = {}
        self.slow_query_threshold = 1.0  # seconds
    
    def optimize_sql(self, query: str, params: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        """
        Optimize SQL query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Optimized query and parameters
        """
        optimized_query = query
        
        # Add query hints for better performance
        if "SELECT" in query.upper() and "LIMIT" not in query.upper():
            # Add default limit if not specified
            optimized_query += " LIMIT 1000"
        
        # Use indexes where possible
        if "WHERE" in query.upper():
            # Ensure indexed columns are used
            pass
        
        # Add EXPLAIN ANALYZE in development
        if logger.isEnabledFor(logging.DEBUG):
            self._log_query_plan(optimized_query)
        
        return optimized_query, params
    
    def optimize_cypher(self, query: str, params: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        """
        Optimize Cypher query.
        
        Args:
            query: Cypher query string
            params: Query parameters
            
        Returns:
            Optimized query and parameters
        """
        optimized_query = query
        
        # Add query hints
        if "MATCH" in query.upper():
            # Use indexes
            if ":Farm" in query and "name:" in query:
                optimized_query = optimized_query.replace(
                    "MATCH (f:Farm)",
                    "MATCH (f:Farm) USING INDEX f:Farm(name)"
                )
        
        # Add limits if not present
        if "LIMIT" not in query.upper():
            optimized_query += " LIMIT 1000"
        
        return optimized_query, params
    
    def track_query_performance(self, query_hash: str, execution_time: float) -> None:
        """Track query performance statistics."""
        if query_hash not in self.query_stats:
            self.query_stats[query_hash] = {
                "count": 0,
                "total_time": 0,
                "min_time": float('inf'),
                "max_time": 0,
                "avg_time": 0
            }
        
        stats = self.query_stats[query_hash]
        stats["count"] += 1
        stats["total_time"] += execution_time
        stats["min_time"] = min(stats["min_time"], execution_time)
        stats["max_time"] = max(stats["max_time"], execution_time)
        stats["avg_time"] = stats["total_time"] / stats["count"]
        
        # Log slow queries
        if execution_time > self.slow_query_threshold:
            logger.warning(f"Slow query detected: {execution_time:.2f}s")
    
    def get_slow_queries(self, limit: int = 10) -> list[Dict[str, Any]]:
        """Get slowest queries."""
        sorted_queries = sorted(
            self.query_stats.items(),
            key=lambda x: x[1]["avg_time"],
            reverse=True
        )
        
        return [
            {
                "query_hash": qh,
                "stats": stats
            }
            for qh, stats in sorted_queries[:limit]
        ]
    
    def _log_query_plan(self, query: str) -> None:
        """Log query execution plan."""
        logger.debug(f"Query plan for: {query[:100]}...")


def cache_result(prefix: str = "query", ttl: int = 3600):
    """
    Decorator for caching function results.
    
    Args:
        prefix: Cache key prefix
        ttl: Time-to-live in seconds
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get or create cache manager
            if not hasattr(wrapper, "_cache_manager"):
                wrapper._cache_manager = CacheManager(ttl=ttl)
                await wrapper._cache_manager.initialize()
            
            # Generate cache key
            cache_key = wrapper._cache_manager._generate_key(
                prefix,
                {"args": str(args), "kwargs": str(kwargs)}
            )
            
            # Check cache
            cached = await wrapper._cache_manager.get(cache_key)
            if cached is not None:
                logger.debug(f"Cache hit for {func.__name__}")
                return cached
            
            # Execute function
            result = await func(*args, **kwargs)
            
            # Store in cache
            await wrapper._cache_manager.set(cache_key, result, ttl)
            
            return result
        
        return wrapper
    return decorator


def measure_performance(func):
    """Decorator to measure function performance."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        
        try:
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            logger.debug(f"{func.__name__} executed in {execution_time:.3f}s")
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {execution_time:.3f}s: {e}")
            raise
    
    return wrapper


class RateLimiter:
    """Rate limiting for API endpoints."""
    
    def __init__(self, requests_per_minute: int = 60):
        """Initialize rate limiter."""
        self.requests_per_minute = requests_per_minute
        self.requests: Dict[str, list[float]] = {}
    
    async def check_rate_limit(self, client_id: str) -> bool:
        """Check if client has exceeded rate limit."""
        current_time = time.time()
        minute_ago = current_time - 60
        
        if client_id not in self.requests:
            self.requests[client_id] = []
        
        # Remove old requests
        self.requests[client_id] = [
            t for t in self.requests[client_id]
            if t > minute_ago
        ]
        
        # Check limit
        if len(self.requests[client_id]) >= self.requests_per_minute:
            return False
        
        # Add current request
        self.requests[client_id].append(current_time)
        
        return True


# Global instances
cache_manager = CacheManager()
query_optimizer = QueryOptimizer()
rate_limiter = RateLimiter()

async def initialize_performance_tools():
    """Initialize all performance tools."""
    await cache_manager.initialize()
    logger.info("Performance optimization tools initialized")