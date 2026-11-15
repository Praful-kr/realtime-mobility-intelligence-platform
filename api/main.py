"""Mobility Intelligence API.   uvicorn api.main:app --port 8000"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from api.config import ApiSettings, load_api_settings
from api.db import Database, DatabaseUnavailable
from api.observability import DB_ERRORS, ObservabilityMiddleware, log, registry
from api.routers import analytics, health


def create_app(settings: Optional[ApiSettings] = None) -> FastAPI:
    settings = settings or load_api_settings()
    db = Database(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        db.close()

    app = FastAPI(title="Mobility Intelligence API", version="1.0.0", lifespan=lifespan,
                  description="Read-only access to Gold tables produced by the streaming pipeline.")
    app.state.db = db
    app.add_middleware(ObservabilityMiddleware)
    app.include_router(health.router)
    app.include_router(analytics.router)

    @app.exception_handler(DatabaseUnavailable)
    async def _db_down(request: Request, exc: DatabaseUnavailable):
        DB_ERRORS.inc()
        log.error("database unavailable", extra={"request_id": getattr(request.state, "request_id", None),
                                                 "error": str(exc)[:300]})
        return JSONResponse(status_code=503, content={"detail": "database unavailable"},
                            headers={"Retry-After": "5"})

    @app.get("/metrics", include_in_schema=False)
    def metrics():
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()
