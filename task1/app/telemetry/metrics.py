from collections import deque
import logging
from typing import Dict, List, Optional
import threading
logger = logging.getLogger("telemetry_metrics")

class TelemetryMetricsTracker:
    LATENCY_WINDOW_SIZE: int = 1000

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._total_requests: int = 0
        self._cache_hits: int = 0
        self._cache_misses: int = 0
        self._latencies_ms: deque = deque(maxlen=self.LATENCY_WINDOW_SIZE)

    def record_query(self, cache_hit: bool, latency_ms: float) -> None:
        with self._lock:
            self._total_requests += 1
            if cache_hit:
                self._cache_hits += 1
            else:
                self._cache_misses += 1

            self._latencies_ms.append(latency_ms)

    def get_summary(self) -> Dict[str, float]:
        with self._lock:
            total = self._total_requests
            hits = self._cache_hits
            misses = self._cache_misses
            latencies : List[float] = list(self._latencies_ms)
        hit_rate = (hits / total * 100.0) if total > 0 else 0.0
        avg_latency = (sum(latencies) / len(latencies)) if latencies else 0.0

        p95_latency = 0.0
        if latencies:
            sorted_latencies = sorted(latencies)
            p95_index = int(len(sorted_latencies) * 0.95)
            p95_latency = sorted_latencies[min(p95_index, len(sorted_latencies) - 1)]

        return {
            "total_queries": float(total),
            "cache_hits": float(hits),
            "cache_misses": float(misses),
            "hit_rate_pct": round(hit_rate, 2),
            "avg_latency_ms": round(avg_latency, 3),
            "p95_latency_ms": round(p95_latency, 3)
        }

    def reset(self) -> None:
        with self._lock:
            self._total_requests = 0
            self._cache_hits = 0
            self._cache_misses = 0
            self._latencies_ms.clear()
            logger.info("Reset telemetry metrics tracker.")


metrics_tracker = TelemetryMetricsTracker()

def get_metrics_tracker() -> TelemetryMetricsTracker:
    return metrics_tracker