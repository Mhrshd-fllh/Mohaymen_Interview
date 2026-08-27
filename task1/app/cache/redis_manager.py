import logging
import time
from typing import Dict, Optional, Tuple
import redis.asyncio as aioredis

from app.config import settings

logger = logging.getLogger("redis_manager")

class RedisManager:

    MAX_CACHE_KEYS: int = 10
    CACHE_TTL_SECONDS: int = 600 # 10 min
    LRU_ZSET_KEY: str = "city_lru_tracker"

    def __init__(self) -> None:
        self.redis: Optional[aioredis.Redis] = None
        self._hits_count: int = 0
        self._misses_count: int = 0

    async def _get_client(self) -> aioredis.Redis:
        if self.redis is None:
            await self.connect()
        assert self.redis is not None, "Redis Connection failed to initialize"
        return self.redis

    async def connect(self) -> None:
        if self.redis is None:
            self.redis = aioredis.Redis(
                host = settings.REDIS_HOST,
                port = settings.REDIS_PORT,
                password = settings.REDIS_PASSWORD,
                decode_responses = True,
                socket_timeout = 5.0
            )
            logger.info("Connected to Redis server at %s:%d", settings.REDIS_HOST, settings.REDIS_PORT)

    async def close(self) -> None:
        if self.redis is not None:
            await self.redis.aclose()
            self.redis = None
            logger.info("Closed Redis Connection Pool.")

    def _format_key(self, city_name: str) -> str:
        # Normalizes city name to lowercase key format
        return f"city:{city_name.strip().lower()}"

    async def get_city(self, city_name: str) -> Optional[str]:
        # Retrieves City's country code from the LRU Cache
        client = await self._get_client()

        key = self._format_key(city_name)
        norm_city = city_name.strip().lower()

        async with client.pipeline(transaction=True) as pipe:
            pipe.get(key)
            results = await pipe.execute()

        value: Optional[str] = results[0]

        if value is not None:
            self._hits_count += 1
            now_ts = time.time()

            async with client.pipeline(transaction=True) as pipe:
                pipe.zadd(self.LRU_ZSET_KEY, {norm_city: now_ts})
                pipe.expire(key, self.CACHE_TTL_SECONDS)
                await pipe.execute()

            logger.debug("Cache HIT for city '%s' -> '%s'", city_name, value)
            return value

        self._misses_count += 1
        logger.debug("Cache MISS for city '%s' -> '%s'", city_name)
        return None

    async def set_city(self, city_name: str, country_code: str) -> Optional[str]:
        # Insert city's country code into the LRU Cache
        client = await self._get_client()

        key = self._format_key(city_name)
        norm_city = city_name.strip().lower()
        now_ts = time.time()
        evicted_city: Optional[str] = None

        async with client.pipeline(transaction=True) as pipe:
            pipe.set(key, country_code.strip().upper(), ex = self.CACHE_TTL_SECONDS)
            pipe.zadd(self.LRU_ZSET_KEY, {norm_city: now_ts})
            pipe.zcard(self.LRU_ZSET_KEY)
            results = await pipe.execute()

        cardinality: int = results[2]

        if cardinality > self.MAX_CACHE_KEYS:
            evicted_items = await client.zpopmin(self.LRU_ZSET_KEY, count = 1)
            if evicted_items:
                raw_evicted, _score = evicted_items[0]
                evicted_city = str(raw_evicted)
                evicted_key = f"City:{evicted_city}"
                await client.delete(evicted_key)
                logger.info(
                    "LRU Eviction Triggered: Cache exceeded %d keys. Evicted oldest key '%s'.",
                    self.MAX_CACHE_KEYS, evicted_city
                )

        logger.debug("Cache SET for city '%s' -> '%s' (TTL=%ds)", city_name, country_code, self.CACHE_TTL_SECONDS)
        return evicted_city

    async def invalidate_city(self, city_name: str) -> bool:
        # Explicitly invalidates a city entry from cache and LRU tracker.

        client = await self._get_client()

        key = self._format_key(city_name)
        norm_city = city_name.strip().lower()

        async with client.pipeline(transaction=True) as pipe:
            pipe.delete(key)
            pipe.zrem(self.LRU_ZSET_KEY, norm_city)
            results = await pipe.execute()

        deleted_count: int = results[0]

        return deleted_count > 0

    async def flush_cache(self) -> None:
        client = await self._get_client()

        tracked_cities = await client.zrange(self.LRU_ZSET_KEY, 0, -1)
        keys_to_delete = [f"City:{city}" for city in tracked_cities]
        keys_to_delete.append(self.LRU_ZSET_KEY)

        if keys_to_delete:
            await client.delete(*keys_to_delete)

        self._hits_count = 0
        self._misses_count = 0
        logger.info("Flushed all city cache keys and reset LRU tracker")

    def get_stats(self) -> Dict[str, float]:
        total = self._hits_count + self._misses_count
        hit_rate = (self._hits_count / total * 100.0) if total > 0 else 0

        return {
            "hits": float(self._hits_count),
            "misses": float(self._misses_count),
            "total_requests": float(total),
            "hit_rate_pct": round(hit_rate, 2)
        }

redis_manager = RedisManager()

def get_redis_manager() -> RedisManager:
    return redis_manager