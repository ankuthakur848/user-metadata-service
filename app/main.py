import time
import uuid
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from .service import create_user_with_retry
from .db import get_user, cb_state, DBWriteError
from .circuit_breaker import CircuitOpenError

app = FastAPI(title="User Metadata Service")

# Mandatory metrics
total_requests = Counter("total_requests", "Total requests")
success_count = Counter("success_count", "Total successful requests")
failure_count = Counter("failure_count", "Total failed requests")
request_latency_ms = Histogram("request_latency_ms", "Request latency in ms")

class UserIn(BaseModel):
    user_id: str
    name: str
    email: EmailStr
    phone: str

@app.middleware("http")
async def metrics_and_logging(request: Request, call_next):
    rid = request.headers.get("x-request-id", str(uuid.uuid4()))
    start = time.time()
    total_requests.inc()

    try:
        response = await call_next(request)
        return response
    except Exception as e:
        failure_count.inc()
        # error summary log
        latency = (time.time() - start) * 1000
        print(f"request_id={rid} method={request.method} path={request.url.path} latency_ms={latency:.2f} error={type(e).__name__}:{e}")
        raise
    finally:
        latency = (time.time() - start) * 1000
        request_latency_ms.observe(latency)
        print(f"request_id={rid} method={request.method} path={request.url.path} latency_ms={latency:.2f} cb_state={cb_state()}")

@app.exception_handler(CircuitOpenError)
async def circuit_open_handler(request: Request, exc: CircuitOpenError):
    failure_count.inc()
    return JSONResponse(status_code=503, content={"error": str(exc), "cb_state": cb_state()})

@app.exception_handler(DBWriteError)
async def db_write_handler(request: Request, exc: DBWriteError):
    failure_count.inc()
    return JSONResponse(status_code=500, content={"error": str(exc)})

@app.get("/healthz")
def healthz():
    return {"ok": True, "cb_state": cb_state()}

@app.get("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

@app.post("/user")
def create_user(payload: UserIn):
    try:
        user = create_user_with_retry(
            user_id=payload.user_id,
            name=payload.name,
            email=str(payload.email),
            phone=payload.phone,
        )
        success_count.inc()
        return user
    except CircuitOpenError:
        raise
    except Exception as e:
        failure_count.inc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/user/{user_id}")
def read_user(user_id: str):
    user = get_user(user_id)
    if not user:
        failure_count.inc()
        raise HTTPException(status_code=404, detail="User not found")
    success_count.inc()
    return user
