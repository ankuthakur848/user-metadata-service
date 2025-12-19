import os
import random
from typing import Dict, Any

from .circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitOpenError

class DBWriteError(Exception):
    pass

# In-memory DB (still fine for assignment)
_DB: Dict[str, Dict[str, Any]] = {}

# Failure injection (simulate DB write failures)
FAIL_RATE = float(os.getenv("FAIL_RATE", "0.0"))  # 0.0 to 1.0
SEED = os.getenv("FAIL_SEED")
if SEED:
    random.seed(int(SEED))

_cb = CircuitBreaker(CircuitBreakerConfig(
    failure_threshold=int(os.getenv("CB_FAILURE_THRESHOLD", "5")),
    recovery_timeout_s=int(os.getenv("CB_RECOVERY_TIMEOUT_S", "20")),
    half_open_successes=int(os.getenv("CB_HALF_OPEN_SUCCESSES", "1")),
))

def cb_state() -> str:
    return _cb.state

def get_user(user_id: str):
    return _DB.get(user_id)

def create_user_idempotent(user_id: str, user_record: Dict[str, Any]):
    """
    DB write with:
      - Circuit breaker protection
      - Simulated failure (FAIL_RATE)
      - Idempotency enforced at DB boundary
    """
    _cb.before_call()

    # Idempotency in DB layer (stronger)
    if user_id in _DB:
        _cb.on_success()
        return _DB[user_id]

    # Simulate intermittent DB failure
    if FAIL_RATE > 0 and random.random() < FAIL_RATE:
        _cb.on_failure()
        raise DBWriteError("Simulated DB write failure")

    _DB[user_id] = user_record
    _cb.on_success()
    return user_record
