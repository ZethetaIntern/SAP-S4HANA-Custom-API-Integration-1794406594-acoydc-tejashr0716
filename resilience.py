"""
ZeTheta Resilience Subsystem — Error Handling, Retry Logic & DLQ
Implements:
1. Exponential backoff retry with jitter
2. Dead-Letter Queue (DLQ) for quarantined poison pills
3. Circuit Breaker Pattern (CLOSED -> OPEN -> HALF-OPEN)
4. Telemetry metrics counters
"""

import time
import random
import logging
from typing import Callable, Any, Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SAP_Resilience")

class DeadLetterQueue:
    def __init__(self, capacity: int = 1000):
        self.capacity = capacity
        self.queue: List[Dict[str, Any]] = []

    def push(self, failed_item: Any, reason: str, attempts: int):
        record = {
            "timestamp": time.time(),
            "item": failed_item,
            "failure_reason": reason,
            "retry_attempts": attempts
        }
        self.queue.append(record)
        logger.error(f"[DLQ] Quarantined defective payload. Reason: {reason} | Total DLQ: {len(self.queue)}")

    def pop_all(self) -> List[Dict[str, Any]]:
        items = list(self.queue)
        self.queue.clear()
        return items

    def size(self) -> int:
        return len(self.queue)

class CircuitBreakerOpenException(Exception):
    pass

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_time_seconds: float = 2.0):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time_seconds
        self.failure_count = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.last_state_change = time.time()

    def record_success(self):
        self.failure_count = 0
        self.state = "CLOSED"

    def record_failure(self):
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            self.last_state_change = time.time()
            logger.warning("[CIRCUIT BREAKER] Tripped to OPEN state! Downstream calls blocked.")

    def can_execute(self) -> bool:
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
    max_delay: float = 0.5
) -> Any:
    """Executes a function with exponential backoff and jitter."""
    attempt = 0
    while True:
        try:
            return func()
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                raise e
            sleep_time = min(max_delay, base_delay * (2 ** (attempt - 1))) + random.uniform(0, 0.02)
            logger.warning(f"Operation failed ({e}). Retrying attempt {attempt}/{max_retries} after {sleep_time:.3f}s...")
            time.sleep(sleep_time)
