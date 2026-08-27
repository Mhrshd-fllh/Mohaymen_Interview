from contextlib import asynccontextmanager
from typing import AsyncGenerator
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.cache.redis_manager import redis_manager
from app.config import settings
from app.database import engine, Base
from app.routers import cities_upsert, cities_query
from app.telemetry.kafka_producer import kafka_producer

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await redis_manager.connect()

    await kafka_producer.start()

    yield

    await kafka_producer.stop()

    await redis_manager.close()
    
    await engine.dispose()

app = FastAPI(
    title="City Country REST API & Caching System",
    description="High-performance FastAPI service featuring atomic PostgreSQL upserts, Redis LRU caching, and Kafka telemetry logging.",
    version="1.0.0",
    lifespan=lifespan
)

# Enable Cross-Origin Resource Sharing (CORS) Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API Routers
app.include_router(cities_upsert.router)
app.include_router(cities_query.router)

@app.get("/health", tags=["Health"])
async def health_check() -> dict:
    """Basic health check endpoint returning API operational status."""
    return {
        "status": "healthy",
        "environment": settings.APP_ENV
    }
