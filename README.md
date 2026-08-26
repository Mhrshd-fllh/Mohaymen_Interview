# Mohaymen_Interview

## Task 1

### Step 1

.env.example is an example of how should .env in task 1 of this project. postgresql + asyncpg is because of FastAPI which is asynchronous and do not block other requests and processes because of connecting to database.

docker-compose file contains three services: PostgreSQL, Redis and kafka. Kafka is in KRaft mode so it is not dependent to ZooKeeper model and use it's own metadata and cluster management which results in low memory usage.

docker compose file also contains healthchecks to repeatively check the services being up.

