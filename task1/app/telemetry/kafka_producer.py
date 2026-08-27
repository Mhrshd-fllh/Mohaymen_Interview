from datetime import datetime, timezone
import json
import logging

import uuid

from typing import Any, Dict, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from app.config import settings

logger = logging.getLogger("kafka_producer")


def json_serializer(value: Dict[str, Any]) -> bytes:
    return json.dumps(value, default=str).encode("utf-8")

class TelemetryKafkaProducer:
    def __init__(self) -> None:
        self.topic: str = settings.KAFKA_TOPIC_TELEMETRY
        self.bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS
        self.producer: Optional[AIOKafkaProducer] = None
        self._is_started: bool = False


    async def start(self) -> None:
        if self._is_started and self.producer is not None:
            return

        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=json_serializer,
                acks=1,
                linger_ms=10,
                max_batch_size=16384,
                retry_backoff_ms=100
            )
            await self.producer.start()
            self._is_started = True
            logger.info("Kafka Producer started successfully. Connected to brokers: %s", self.bootstrap_servers)
        except Exception as e:
            logger.warning("Failed to connect to kafka brookers (%s): %s. Telemetry logging degraded.", self.bootstrap_servers, str(e))
            self.producer = None
            self._is_started = False

    async def stop(self) -> None:
        if self.producer is not None and self._is_started:
            try:
                await self.producer.stop()
                logger.info("Kafka Producer Shut Down Completely.")
            except Exception as e:
                logger.error("Error during kafka prodcuer shut down: %s", str(e))
            finally:
                self.producer = None
                self._is_started = False

    async def send_telemetry_log(
            self,
            city_name: str,
            country_code: Optional[str],
            cache_hit: bool,
            latency_ms: float
    ) -> bool:
        if not self._is_started or self.producer is None:
            await self.start()
            if not self._is_started or self.producer is None:
                return False

        payload: Dict[str, Any] = {
            "event_id": str(uuid.uuid4()),
            "city_name": city_name,
            "country_code": country_code,
            "cache_hit": cache_hit,
            "latency_ms": round(latency_ms, 3),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        try:
            await self.producer.send_and_wait(
                self.topic,
                value=payload,
                key=city_name.lower().encode("utf-8")
            )
            logger.debug("Telemetry log emitted to kafka topic '%s' for city '%s'", self.topic, city_name)
            return True
        except KafkaError as e:
            logger.error("Kafka delivery error emitting telemetry for city '%s': %s", city_name, str(e))
            return False
        except Exception as e:
            logger.error("Unexpected error sending kafka telemetry event: %s", str(e))
            return False

kafka_producer = TelemetryKafkaProducer()

def get_kafka_producer() -> TelemetryKafkaProducer:
    return kafka_producer
