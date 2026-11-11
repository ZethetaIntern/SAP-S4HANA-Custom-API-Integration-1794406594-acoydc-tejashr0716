"""
ZeTheta Resilience Subsystem — Error Handling, Retry Logic & DLQ
Implements:
1. Exponential backoff retry with jitter
2. Dead-Letter Queue (DLQ) with automated re-ingestion / retry capabilities
3. Circuit Breaker Pattern (CLOSED -> OPEN -> HALF-OPEN) with manual fault injection
4. Structured JSON Logging with Correlation ID support
"""

import time
import json
import random
import logging
from typing import Callable, Any, Dict, List, Optional

class StructuredJsonFormatter(logging.Formatter):
    """Formats log records as structured JSON for enterprise observability."""
    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", "SYSTEM_INTERNAL")
        }
        if hasattr(record, "extra_data") and isinstance(record.extra_data, dict):
            log_obj["details"] = record.extra_data
        return json.dumps(log_obj)

# Configure logger
logger = logging.getLogger("SAP_Resilience")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(StructuredJsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

class DeadLetterQueue:
    """Enterprise Dead-Letter Queue with re-ingestion and quarantine capabilities."""
    def __init__(self, capacity: int = 1000):
        self.capacity = capacity
        self.queue: List[Dict[str, Any]] = []

    def push(self, failed_item: Any, reason: str, attempts: int, correlation_id: str = "N/A"):
        record = {
            "id": f"DLQ_{int(time.time()*1000)}_{random.randint(100, 999)}",
            "timestamp": time.time(),
            "item": failed_item,
            "failure_reason": reason,
            "retry_attempts": attempts,
            "correlation_id": correlation_id
        }
        self.queue.append(record)
        logger.error(
            f"[DLQ] Quarantined defective payload: {reason}",
            extra={"extra_data": {"dlq_id": record["id"], "total_dlq": len(self.queue)}, "correlation_id": correlation_id}
        )

    def pop_all(self) -> List[Dict[str, Any]]:
        items = list(self.queue)
        self.queue.clear()
        return items

    def retry_all(self, reprocess_fn: Optional[Callable[[Dict[str, Any]], bool]] = None) -> int:
        """
        Attempts to reprocess queued items.
        If reprocess_fn is provided and returns True, item is dequeued.
        If reprocess_fn is None, all items are cleared and counted as reprocessed.
        """
        if not self.queue:
            return 0
        if reprocess_fn is None:
            count = len(self.queue)
            self.queue.clear()
            logger.info(f"[DLQ] Flushed and reprocessed {count} quarantined items.")
            return count

        remaining = []
        reprocessed_count = 0
        for record in self.queue:
            try:
                if reprocess_fn(record["item"]):
                    reprocessed_count += 1
                else:
                    remaining.append(record)
            except Exception as e:
                logger.warning(f"[DLQ] Retry failed for {record.get('id')}: {e}")
                remaining.append(record)

        self.queue = remaining
        logger.info(f"[DLQ] Reprocessed {reprocessed_count} items. Remaining: {len(self.queue)}")
        return reprocessed_count

    def size(self) -> int:
        return len(self.queue)

class CircuitBreakerOpenException(Exception):
    pass

class CircuitBreaker:
    """3-State Circuit Breaker (CLOSED -> OPEN -> HALF_OPEN) with fault-injection simulation."""
    def __init__(self, failure_threshold: int = 3, recovery_time_seconds: float = 2.0):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time_seconds
        self.failure_count = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.last_state_change = time.time()
        self.simulated_failure_active = False

    def record_success(self):
        if not self.simulated_failure_active:
            self.failure_count = 0
            self.state = "CLOSED"

    def record_failure(self):
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            self.last_state_change = time.time()
            logger.warning("[CIRCUIT BREAKER] Tripped to OPEN state! Downstream calls blocked.")

    def force_trip(self, enable_simulation: bool = True):
        """Fault-injection trigger: forces circuit breaker to OPEN state."""
        self.simulated_failure_active = enable_simulation
        self.state = "OPEN"
        self.failure_count = self.failure_threshold
        self.last_state_change = time.time()
        logger.warning("[CIRCUIT BREAKER] Fault injection activated: Breaker forced OPEN.")

    def reset(self):
        """Restores circuit breaker to normal operational state."""
        self.simulated_failure_active = False
        self.failure_count = 0
        self.state = "CLOSED"
        self.last_state_change = time.time()
        logger.info("[CIRCUIT BREAKER] Circuit breaker manually reset to CLOSED.")

    def can_execute(self) -> bool:
        if self.simulated_failure_active:
            return False
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if time.time() - self.last_state_change > self.recovery_time:
                self.state = "HALF_OPEN"
                logger.info("[CIRCUIT BREAKER] Entering HALF_OPEN canary trial.")
                return True
            return False
        if self.state == "HALF_OPEN":
            return True
        return False

def execute_with_retry(
    func: Callable[[], Any],
    max_retries: int = 3,
    base_delay: float = 0.05,
    max_delay: float = 0.5,
    correlation_id: str = "N/A"
) -> Any:
    """Executes a function with exponential backoff and jitter."""
    attempt = 0
    while True:
        try:
            return func()
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                logger.error(
                    f"[RETRY_EXHAUSTED] Maximum retries ({max_retries}) reached: {e}",
                    extra={"correlation_id": correlation_id}
                )
                raise e
            sleep_time = min(max_delay, base_delay * (2 ** (attempt - 1))) + random.uniform(0, 0.02)
            logger.warning(
                f"[RETRY] Attempt {attempt}/{max_retries} failed ({e}). Backing off {sleep_time:.3f}s...",
                extra={"correlation_id": correlation_id}
            )
            time.sleep(sleep_time)
