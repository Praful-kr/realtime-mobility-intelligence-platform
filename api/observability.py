"""Request-id + structured access log + Prometheus metrics middleware."""
from __future__ import annotations

import time
import uuid

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from common.logging_setup import get_logger

registry = CollectorRegistry()
REQUESTS = Counter("api_http_requests_total", "HTTP requests", ["method", "route", "status"], registry=registry)
LATENCY = Histogram("api_http_request_duration_seconds", "HTTP request latency", ["route"], registry=registry,
                    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
INFLIGHT = Gauge("api_http_requests_in_flight", "In-flight HTTP requests", registry=registry)
DB_ERRORS = Counter("api_db_errors_total", "Requests failed because the database was unavailable", registry=registry)

log = get_logger("api")


class ObservabilityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = rid
        INFLIGHT.inc()
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["x-request-id"] = rid
            return response
        finally:
            elapsed = time.perf_counter() - start
            INFLIGHT.dec()
            route = getattr(request.scope.get("route"), "path", None) or "unmatched"  # template, not raw path
            REQUESTS.labels(request.method, route, str(status)).inc()
            LATENCY.labels(route).observe(elapsed)
            if route != "/metrics":
                log.info("request", extra={"request_id": rid, "method": request.method, "path": request.url.path,
                                           "route": route, "status": status, "duration_ms": round(elapsed * 1000, 2),
                                           "query": request.url.query[:200]})
