# Mohaymen_Interview

This repository contains the solution for the Mohaymen Data Engineering interview project. The project is divided into two tasks:
- Task 1: A FastAPI web service with PostgreSQL, Redis LRU cache, and Kafka telemetry.
- Task 2: Stream processing with Apache Spark Structured Streaming and MinIO object storage.

---

## Task 1 - REST API, Caching and Telemetry

### Step 1: Environment and Docker Infrastructure

In the `task1` folder, `.env.example` is an example of how `.env` should be in task 1 of this project.

We use `postgresql+asyncpg` because FastAPI is asynchronous, so connecting and querying the database does not block other requests and processes.

The `docker-compose.yml` file contains four services:
- PostgreSQL: The main database for storing city and country records.
- Redis: In-memory cache for fast lookups.
- Kafka: Used for telemetry events. Kafka is in KRaft mode, so it is not dependent on the ZooKeeper model and uses its own metadata and cluster management, which results in low memory usage and quick startup.
- API: The containerized FastAPI application running with Uvicorn.

The docker-compose file also contains healthchecks to repeatedly check that PostgreSQL, Redis, and Kafka are healthy before the API container starts (`depends_on: condition: service_healthy`).

### Step 2: Database Connection, Models and Schemas

In the `task1/app` directory, we have:
- `config.py`: Loads environment variables from `.env` using Pydantic Settings.
- `database.py`: Sets up the asynchronous database connection pool (`asyncpg`) and sessionmaker. It configures connection pool settings like `pool_size=20`, `max_overflow=10`, `pool_recycle=1800`, and `pool_pre_ping=True` to keep connections healthy and prevent stale connection drops.
- `models.py`: Uses SQLAlchemy ORM to create the `city_country` table in PostgreSQL. It defines columns `city_name` and `country_code`, and adds a functional B-Tree index on `lower(city_name)` for fast, case-insensitive searches.
- `schemas.py`: Uses Pydantic for request and response validation. It trims extra spaces and converts country codes to uppercase.

### Step 3: Upsert Endpoint (`POST /cities`)

In `task1/app/routers/cities_upsert.py`, we created the `POST /cities` endpoint:
- It accepts a JSON body with `city` and `country_code`.
- It uses PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` (atomic upsert). If the city does not exist in the database, it inserts it. If it already exists, it updates the country code. This avoids race conditions when multiple requests come at the same time.
- After a successful upsert, it removes the city from Redis cache so the cache does not return old data.

### Step 4: Seeding Script

In `task1/scripts/seed_cities.py`, we wrote a standalone Python script to seed the database:
- It reads city data from the local `Cities` directory.
- It sends asynchronous HTTP requests using `httpx` to the `POST /cities` endpoint.
- It uses batching and an async semaphore so it does not send too many requests at once and does not overload the API server.

### Step 5: Redis LRU Cache

In `task1/app/cache/redis_manager.py`, we implemented a custom LRU (Least Recently Used) cache:
- The cache can hold at most 10 keys.
- Each key has a Time-To-Live (TTL) of 10 minutes (600 seconds).
- To make LRU eviction deterministic, we use a Redis Sorted Set (`ZSET`) called `city_lru_tracker` to track the last access timestamp for each city.
- When the cache size reaches more than 10 keys, the manager finds the key with the oldest timestamp and deletes it.

### Step 6: Kafka Telemetry Producer and Metrics

In `task1/app/telemetry/`:
- `kafka_producer.py`: An asynchronous Kafka producer using `aiokafka`. When a city lookup is made, it sends a JSON event with the city name, country code, cache hit or miss status, and request latency to the `city_telemetry` topic.
- `metrics.py`: An in-memory tracker that computes total requests, cache hits, cache misses, hit rate percentage, and rolling average latency.

### Step 7: Query Endpoint (`GET /cities/{city_name}`)

In `task1/app/routers/cities_query.py`, we built the complete query endpoint:
1. It checks the Redis LRU cache first. If found, it returns the result immediately (Cache HIT).
2. If not found in cache (Cache MISS), it queries PostgreSQL.
3. If found in PostgreSQL, it saves the result into Redis for future requests.
4. If not found in PostgreSQL, it returns a 404 error.
5. Sending logs to Kafka and updating metrics is run in a FastAPI `BackgroundTask`, so the response is returned to the user quickly without waiting for Kafka.
- It also provides `GET /cities/metrics/summary` to view current cache hit rates and latency numbers.

---

## Task 2 - Stream Processing with Spark and MinIO

### Step 1: MinIO and Spark Session Setup

In the `task2` folder:
- `docker-compose.yml`: Runs a MinIO container, which is an S3-compatible local object storage.
- `src/spark_session.py`: Creates a PySpark session configured with Hadoop AWS and S3A connectors to read from and write to MinIO.
- `src/minio_utils.py`: Utility functions to check and create the required buckets in MinIO before jobs start.

### Step 2: Streaming Schema and Transformers

In `task2/src/pipeline/`:
- `schema.py`: Defines the schema for streaming SMS data in `REF_SMS` and the schema for the static reference table.
- `transformers.py`: Parses the event timestamps into standard timestamp format and converts currency values from milli-Rials to Tomans (dividing by 10,000).

### Step 3: Report 1 - Daily Total Revenue

In `task2/jobs/report_1_daily_revenue.py`:
- Streams SMS records using Spark Structured Streaming.
- Adds a watermark on the event timestamp to handle late-arriving records.
- Groups data by day and sums total revenue in Tomans.
- Writes the daily output to MinIO in CSV format using streaming checkpoints.

### Step 4: Report 2 - 15-Minute Window Revenue by Paytype

In `task2/jobs/report_2_windowed_revenue.py`:
- Divides data into 15-minute tumbling windows based on event time.
- Groups by the 15-minute window and `paytype`.
- Calculates the total revenue for each payment type in each window and saves the results to MinIO.

### Step 5: Report 3 - Minimum and Maximum Revenue

In `task2/jobs/report_3_min_max_revenue.py`:
- Uses the same 15-minute tumbling windows grouped by `paytype`.
- Calculates both the minimum and maximum revenue in each window for each payment type.
- Saves the metrics to MinIO.

### Step 6: Report 4 - Reference Table Join and Summary

In `task2/jobs/report_4_reference_enriched_summary.py`:
- Performs a stream-to-static join between live SMS records and the static reference table.
- Converts numeric `paytype` IDs into readable names like Prepaid and Postpaid.
- Computes both total record counts and total revenue per window and payment name, then writes the result to MinIO.

---

## How to Run the Project

### Task 1

You can run Task 1 using Docker Compose for all services, or run the background services in Docker and run FastAPI locally.

**Option A: Run everything with Docker Compose**

1. Go to the `task1` folder:
   ```bash
   cd task1
   ```
2. Copy the environment file:
   ```bash
   cp .env.example .env
   ```
3. Build and start all containers (Postgres, Redis, Kafka, and API):
   ```bash
   docker compose up -d --build
   ```
4. Seed city data into the database:
   ```bash
   python scripts/seed_cities.py
   ```
5. Open the Swagger UI in your browser:
   ```text
   http://localhost:8000/docs
   ```

**Option B: Run background services in Docker and run FastAPI locally**

1. Go to `task1` and set up `.env`:
   ```bash
   cd task1
   cp .env.example .env
   ```
2. Start only the background services:
   ```bash
   docker compose up postgres redis kafka -d
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the FastAPI application locally:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   ```
5. Seed city data:
   ```bash
   python scripts/seed_cities.py
   ```

### Task 2

1. Open a terminal and go to the `task2` folder:
   ```bash
   cd task2
   ```
2. Copy the environment file:
   ```bash
   cp .env.example .env
   ```
3. Start MinIO with docker-compose:
   ```bash
   docker compose up -d
   ```
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
5. Run any streaming job:
   ```bash
   python jobs/report_1_daily_revenue.py
   python jobs/report_2_windowed_revenue.py
   python jobs/report_3_min_max_revenue.py
   python jobs/report_4_reference_enriched_summary.py
   ```
