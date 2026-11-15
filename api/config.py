"""API settings from environment (12-factor)."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ApiSettings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    pool_min: int
    pool_max: int
    statement_timeout_ms: int
    log_level: str
    acquire_timeout_s: float = 3.0


def load_api_settings() -> ApiSettings:
    return ApiSettings(
        db_host=os.getenv("API_DB_HOST", "localhost"),
        db_port=int(os.getenv("API_DB_PORT", "5433")),
        db_name=os.getenv("API_DB_NAME", os.getenv("POSTGRES_DB", "mobility")),
        db_user=os.getenv("API_DB_USER", "api_ro"),
        db_password=os.getenv("API_DB_PASSWORD", "api_ro_dev"),
        pool_min=int(os.getenv("API_DB_POOL_MIN", "1")),
        pool_max=int(os.getenv("API_DB_POOL_MAX", "10")),
        statement_timeout_ms=int(os.getenv("API_STATEMENT_TIMEOUT_MS", "5000")),
        log_level=os.getenv("API_LOG_LEVEL", "INFO"),
        acquire_timeout_s=float(os.getenv("API_DB_ACQUIRE_TIMEOUT_S", "3")),
    )
