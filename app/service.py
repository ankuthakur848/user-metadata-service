import time
from tenacity import retry, stop_after_attempt, wait_exponential, wait_random, retry_if_exception_type, before_sleep_log
import logging

from .db import create_user_idempotent, DBWriteError
from .circuit_breaker import CircuitOpenError

logger = logging.getLogger("user-metadata-service")
logger.setLevel(logging.INFO)

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.2, min=0.2, max=2) + wait_random(0, 0.3),
    retry=retry_if_exception_type(DBWriteError),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def create_user_with_retry(user_id: str, name: str, email: str, phone: str):
    user_record = {
        "user_id": user_id,
        "name": name,
        "email": email,
        "phone": phone,
        "created_at": time.time(),
    }
    # This call may raise DBWriteError (retryable) or CircuitOpenError (not retryable)
    return create_user_idempotent(user_id, user_record)
