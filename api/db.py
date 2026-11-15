"""Lazy, thread-safe Postgres pool. The process starts (and stays alive) without the database; readiness
reports the outage and requests fail with 503 instead of crashing the server."""
from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Sequence

import psycopg2
from psycopg2 import pool as pgpool
from psycopg2.extras import RealDictCursor

from api.config import ApiSettings

# Fixed whitelist: table names are NEVER taken from user input.
TABLES = {"minute": "gold.trips_per_minute", "sliding": "gold.demand_sliding_15m", "hourly": "gold.hourly_demand",
          "zone": "gold.zone_demand_5m", "anomaly": "gold.anomaly_counts"}


class DatabaseUnavailable(Exception):
    pass


class Database:
    def __init__(self, settings: ApiSettings):
        self.s = settings
        self._pool: Optional[pgpool.ThreadedConnectionPool] = None
        self._lock = threading.Lock()
        # psycopg2's pool raises instantly when empty. Queue requests for a free connection instead (bounded wait);
        # only a wait longer than acquire_timeout_s becomes a 503 (saturation), not a mere burst.
        self._slots = threading.BoundedSemaphore(settings.pool_max)

    def _get_pool(self) -> pgpool.ThreadedConnectionPool:
        with self._lock:
            if self._pool is None:
                try:
                    self._pool = pgpool.ThreadedConnectionPool(
                        self.s.pool_min, self.s.pool_max, host=self.s.db_host, port=self.s.db_port,
                        dbname=self.s.db_name, user=self.s.db_user, password=self.s.db_password,
                        connect_timeout=3, application_name="mobility-api",
                        options=f"-c statement_timeout={self.s.statement_timeout_ms} -c timezone=UTC "
                                f"-c default_transaction_read_only=on")
                except psycopg2.Error as e:
                    raise DatabaseUnavailable(str(e).strip()) from e
            return self._pool

    @contextmanager
    def cursor(self) -> Iterator[RealDictCursor]:
        if not self._slots.acquire(timeout=self.s.acquire_timeout_s):
            raise DatabaseUnavailable("connection pool saturated")
        try:
            p = self._get_pool()
            try:
                conn = p.getconn()
            except pgpool.PoolError as e:
                raise DatabaseUnavailable("connection pool exhausted") from e
            broken = False
            try:
                conn.autocommit = True
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    yield cur
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                broken = True
                raise DatabaseUnavailable(str(e).strip()) from e
            finally:
                p.putconn(conn, close=broken)
        finally:
            self._slots.release()

    def query(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def latest(self, table_key: str, column: str = "window_start") -> Optional[datetime]:
        """Newest window in a table; used to anchor default time ranges (replayed data is historical,
        so 'last hour' means 'last hour of data', not wall-clock)."""
        rows = self.query(f"SELECT max({column}) AS m FROM {TABLES[table_key]}", {})
        return rows[0]["m"]

    def close(self) -> None:
        if self._pool is not None:
            self._pool.closeall()
