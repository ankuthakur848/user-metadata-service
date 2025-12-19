import time
from dataclasses import dataclass

class CircuitOpenError(Exception):
    pass

@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5        # open circuit after N consecutive failures
    recovery_timeout_s: int = 20      # stay OPEN for this many seconds
    half_open_successes: int = 1      # successes needed in HALF_OPEN to close

class CircuitBreaker:
    """
    Simple circuit breaker:
      - CLOSED: allow requests; count failures
      - OPEN: reject requests until recovery timeout expires
      - HALF_OPEN: allow limited requests; close on success, open on failure
    """
    def __init__(self, cfg: CircuitBreakerConfig):
        self.cfg = cfg
        self.state = "CLOSED"
        self.failures = 0
        self.opened_at = 0.0
        self.half_open_success = 0

    def before_call(self):
        if self.state == "OPEN":
            if (time.time() - self.opened_at) >= self.cfg.recovery_timeout_s:
                self.state = "HALF_OPEN"
                self.half_open_success = 0
            else:
                raise CircuitOpenError("Circuit breaker is OPEN (DB calls blocked)")

    def on_success(self):
        if self.state == "HALF_OPEN":
            self.half_open_success += 1
            if self.half_open_success >= self.cfg.half_open_successes:
                self.state = "CLOSED"
                self.failures = 0
        else:
            self.failures = 0

    def on_failure(self):
        if self.state == "HALF_OPEN":
            self.state = "OPEN"
            self.opened_at = time.time()
            self.failures = 0
            return

        self.failures += 1
        if self.failures >= self.cfg.failure_threshold:
            self.state = "OPEN"
            self.opened_at = time.time()
