from datetime import datetime, timezone
import time
from typing import Dict, Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache.redis_manager import RedisManager, get_redis_manager
from app.database import get_db_session
from app.models import CityCountry
from app.schemas import CityResponse
from app.telemetry.kafka_producer import TelemetryKafkaProducer, get_kafka_producer
from app.telemetry.metrics import TelemetryMetricsTracker, get_metrics_tracker

router = APIRouter(prefix="/cities", tags =["Cities"])


async def log_telemetry_background(
        kafka_producer: TelemetryKafkaProducer,
        metrics_tracker: TelemetryMetricsTracker,
        city_name: str,
        country_code: str | None,
        cache_hit: bool,
        latency_ms: float
) -> None:
    metrics_tracker.record_query(cache_hit=cache_hit, latency_ms=latency_ms)

    await kafka_producer.send_telemetry_log(
        city_name = city_name,
        country_code=country_code,
        cache_hit=cache_hit,
        latency_ms=latency_ms
    )

@router.get("/metrics/summary", summary="Get aggregated telemetry & cache metrics", 
            description="Returns real-time cache hit rates, total request volumes, averge/P95 latencies, and Redis LRU Cache")
async def get_telemetry_metrics(
    metrics_tracker: TelemetryMetricsTracker = Depends(get_metrics_tracker),
    cache_manager: RedisManager = Depends(get_redis_manager)
) -> Dict[str, Any]:
    summary =  metrics_tracker.get_summary()
    cache_stats = cache_manager.get_stats()
    cardinality = 0
    if cache_manager.redis is not None:
        try:
            cardinality = await cache_manager.redis.zcard(cache_manager.LRU_ZSET_KEY)
        except Exception:
            cardinality = 0

    return {
        "telemetry_metrics": summary,
        "try_cache_stats": {
            **cache_stats,
            "active_cached_keys": cardinality,
            "max_cache_keys": cache_manager.MAX_CACHE_KEYS,
            "ttl_seconds": cache_manager.CACHE_TTL_SECONDS
        }
    }

@router.get("/{city_name}", response_model=CityResponse,
            status_code=status.HTTP_200_OK,
            summary="Query country code by City Name",
            description="Chained lookup pipeline: Redis LRU Cache -> PostgreSQL DB Fallback -> Redis Write-Through -> Async Kafka Telemetry."
            )
async def query_city_by_name(
    city_name: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db_session),
    cache_manager: RedisManager = Depends(get_redis_manager),
    kafka_producer: TelemetryKafkaProducer = Depends(get_kafka_producer),
    metrics_tracker: TelemetryMetricsTracker = Depends(get_metrics_tracker)
) -> CityResponse:
    t0 = time.perf_counter()
    clean_city = city_name.strip()
    cached_country_code = await cache_manager.get_city(clean_city)
    if cached_country_code is not None:
        latency_ms = (time.perf_counter() - t0) *1000.0

        background_tasks.add_task(
            log_telemetry_background,
            kafka_producer,
            metrics_tracker,
            clean_city,
            cached_country_code,
            True,
            latency_ms
        )

        now_utc = datetime.now(timezone.utc)

        return CityResponse(
            id = 0,
            city_name=clean_city,
            country_code=cached_country_code,
            created_at=now_utc,
            updated_at=now_utc,
            message="Cache hit"
        )

    db_stmt = select(CityCountry).where(
        func.lower(CityCountry.city_name) == func.lower(clean_city)
    )
    db_result = await db.execute(db_stmt)
    city_record = db_result.scalar_one_or_none()

    if city_record is None:
        latency_ms = (time.perf_counter() - t0) * 1000.0

        background_tasks.add_task(
            log_telemetry_background,
            kafka_producer,
            metrics_tracker,
            clean_city,
            None,
            False,
            latency_ms
        )
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail=f"City '{clean_city}' not found in database records."
        )


    await cache_manager.set_city(city_record.city_name, city_record.country_code)
    latency_ms = (time.perf_counter() - t0) * 1000.0

    background_tasks.add_task(
        log_telemetry_background,
        kafka_producer,
        metrics_tracker,
        city_record.city_name,
        city_record.country_code,
        False,
        latency_ms
    )

    return CityResponse(
        id = city_record.id,
        city_name=city_record.city_name,
        country_code= city_record.country_code,
        created_at = city_record.created_at,
        updated_at=city_record.updated_at,
        message = "Cache Miss"
    )

