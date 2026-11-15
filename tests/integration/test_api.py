"""API integration tests against a REAL Postgres (throwaway database, real DDL, real read-only role).
Needs the compose stack's Postgres on localhost:5433; skipped with a clear reason if unreachable."""
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import pytest
from fastapi.testclient import TestClient

from api.config import ApiSettings
from api.main import create_app

UTC = timezone.utc
HOST, PORT = "localhost", int(os.getenv("PG_HOST_PORT", "5433"))
ADMIN = dict(host=HOST, port=PORT, user=os.getenv("POSTGRES_USER", "mobility"),
             password=os.getenv("POSTGRES_PASSWORD", "mobility_dev"), dbname="postgres", connect_timeout=3)
DDL = (Path(__file__).parents[2] / "infrastructure/postgres/init.sql").read_text()


def t(day, h, m=0):
    return datetime(2024, 1, day, h, m, tzinfo=UTC)


def make_db(name, seed):
    try:
        admin = psycopg2.connect(**ADMIN)
    except psycopg2.OperationalError as e:
        pytest.skip(f"Postgres not reachable on {HOST}:{PORT} (start with `make up`): {e}")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {name}")
    admin.close()
    conn = psycopg2.connect(**{**ADMIN, "dbname": name})
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(DDL)
        if seed:
            seed_rows(cur)
    conn.close()


def drop_db(name):
    admin = psycopg2.connect(**ADMIN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    admin.close()


def seed_rows(cur):
    m = ("INSERT INTO gold.trips_per_minute (window_start, taxi_type, trips, revenue, fare_sum, tip_sum, distance_sum,"
         " duration_sum_s, duration_n, avg_duration_s, avg_fare) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
    cur.execute(m, (t(5, 10, 0), "yellow", 10, 200.0, 150.0, 20.0, 30.0, 6000, 10, 600.0, 15.0))
    cur.execute(m, (t(5, 10, 0), "green", 2, 40.0, 30.0, 4.0, 6.0, 1200, 2, 600.0, 15.0))
    cur.execute(m, (t(5, 10, 1), "yellow", 20, 400.0, 300.0, 40.0, 60.0, 12000, 20, 600.0, 15.0))
    cur.execute(m, (t(5, 11, 30), "yellow", 5, 100.0, 75.0, 10.0, 15.0, 3000, 5, 600.0, 15.0))
    # 70 steady minutes on Jan 4 with one 10x spike at 01:05 (feeds gold.v_demand_anomalies); earlier than the rest
    for i in range(70):
        trips = 200 if i == 65 else (20 if i % 2 == 0 else 22)
        cur.execute(m, (t(4, 0) + timedelta(minutes=i), "yellow", trips, trips * 20.0, 0.0, 0.0, 0.0, 0, 0, None, None))
    z = "INSERT INTO gold.zone_demand_5m (window_start, zone_id, role, borough, zone_name, trips, revenue) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    for ws, zid, role, b, n, trips, rev in [
            (t(5, 10, 0), 161, "pickup", "Manhattan", "Midtown Center", 5, 100.0),
            (t(5, 10, 5), 161, "pickup", "Manhattan", "Midtown Center", 7, 140.0),
            (t(5, 10, 0), 132, "pickup", "Queens", "JFK Airport", 3, 270.0),
            (t(5, 10, 5), 132, "pickup", "Queens", "JFK Airport", 3, 270.0),
            (t(5, 10, 0), 161, "dropoff", "Manhattan", "Midtown Center", 2, 40.0)]:
        cur.execute(z, (ws, zid, role, b, n, trips, rev))
    cur.execute("INSERT INTO gold.demand_sliding_15m (window_start, window_end, trips, revenue) VALUES (%s,%s,%s,%s)",
                (t(5, 10, 0), t(5, 10, 15), 32, 640.0))
    h = "INSERT INTO gold.hourly_demand (window_start, taxi_type, trips, revenue, fare_sum) VALUES (%s,%s,%s,%s,%s)"
    cur.execute(h, (t(5, 10), "yellow", 30, 600.0, 450.0))
    cur.execute(h, (t(6, 10), "yellow", 10, 100.0, 80.0))
    cur.execute(h, (t(5, 22), "green", 4, 60.0, 50.0))
    a = "INSERT INTO gold.anomaly_counts (window_start, anomaly_type, n) VALUES (%s,%s,%s)"
    cur.execute(a, (t(5, 10, 0), "speed_over_80mph", 2))
    cur.execute(a, (t(5, 10, 1), "tip_over_100", 1))
    s = ("INSERT INTO gold.stream_metrics (query_name, run_id, batch_id, input_rows, dropped_by_watermark, state_rows,"
         " trigger_ms, processed_rows_per_s, ts) VALUES (%s,'r',%s,%s,%s,%s,%s,%s,%s)")
    cur.execute(s, ("gold_trips_per_minute", 1, 1000, 0, 10, 500, 2000.0, t(5, 12, 0)))
    cur.execute(s, ("gold_trips_per_minute", 2, 500, 3, 12, 400, 1250.0, t(5, 12, 5)))
    cur.execute(s, ("gold_hourly_demand", 1, 700, 1, 4, 300, 2300.0, t(5, 12, 1)))


def settings(db, port=PORT):
    return ApiSettings(db_host=HOST, db_port=port, db_name=db, db_user="api_ro", db_password="api_ro_dev",
                       pool_min=1, pool_max=4, statement_timeout_ms=5000, log_level="INFO")


@pytest.fixture(scope="module")
def client():
    make_db("mobility_api_test", seed=True)
    with TestClient(create_app(settings("mobility_api_test"))) as c:
        yield c
    drop_db("mobility_api_test")


@pytest.fixture(scope="module")
def empty_client():
    make_db("mobility_api_empty", seed=False)
    with TestClient(create_app(settings("mobility_api_empty"))) as c:
        yield c
    drop_db("mobility_api_empty")


RANGE = {"start": "2024-01-05T10:00:00Z", "end": "2024-01-05T12:00:00Z"}


# ---- health ---------------------------------------------------------------------------------------------
def test_liveness_and_readiness(client):
    assert client.get("/health/live").json() == {"status": "alive"}
    r = client.get("/health/ready")
    assert r.status_code == 200 and r.json() == {"status": "ready", "checks": {"database": "ok", "schema": "ok"}}


def test_database_down_means_live_ok_ready_503_data_503(client):
    with TestClient(create_app(settings("mobility_api_test", port=1))) as down:   # nothing listens on port 1
        assert down.get("/health/live").status_code == 200
        r = down.get("/health/ready")
        assert r.status_code == 503 and r.json()["status"] == "not_ready"
        d = down.get("/v1/trips/per-minute", params=RANGE)
        assert d.status_code == 503 and d.headers["retry-after"] == "5"


# ---- data endpoints -------------------------------------------------------------------------------------
def test_per_minute_values_and_order(client):
    r = client.get("/v1/trips/per-minute", params=RANGE)
    assert r.status_code == 200
    rows = r.json()["data"]
    assert [(x["window_start"], x["taxi_type"], x["trips"]) for x in rows] == [
        ("2024-01-05T10:00:00Z", "green", 2), ("2024-01-05T10:00:00Z", "yellow", 10),
        ("2024-01-05T10:01:00Z", "yellow", 20), ("2024-01-05T11:30:00Z", "yellow", 5)]
    y = rows[1]
    assert (y["revenue"], y["avg_fare"], y["avg_duration_s"], y["tip_sum"], y["distance_sum"]) == (200.0, 15.0, 600.0, 20.0, 30.0)
    assert r.json()["meta"]["count"] == 4


def test_filter_limit_and_exclusive_end(client):
    only_green = client.get("/v1/trips/per-minute", params={**RANGE, "taxi_type": "green"}).json()["data"]
    assert [x["trips"] for x in only_green] == [2]
    assert len(client.get("/v1/trips/per-minute", params={**RANGE, "limit": 2}).json()["data"]) == 2
    end_excl = client.get("/v1/trips/per-minute", params={"start": RANGE["start"], "end": "2024-01-05T10:01:00Z"})
    assert {x["window_start"] for x in end_excl.json()["data"]} == {"2024-01-05T10:00:00Z"}


def test_default_window_is_anchored_on_latest_data_not_wall_clock(client):
    body = client.get("/v1/trips/per-minute").json()
    assert [x["window_start"] for x in body["data"]] == ["2024-01-05T11:30:00Z"]   # newest hour of DATA
    assert body["meta"]["end"] == "2024-01-05T11:30:01Z"


def test_naive_timestamps_are_utc(client):
    a = client.get("/v1/trips/per-minute", params={"start": "2024-01-05T10:00:00", "end": "2024-01-05T10:01:00"})
    b = client.get("/v1/trips/per-minute", params={"start": "2024-01-05T10:00:00Z", "end": "2024-01-05T10:01:00Z"})
    assert a.json()["data"] == b.json()["data"] and len(a.json()["data"]) == 2


def test_sliding_hourly_and_hour_of_day(client):
    s = client.get("/v1/demand/sliding", params=RANGE).json()["data"]
    assert s == [{"window_start": "2024-01-05T10:00:00Z", "window_end": "2024-01-05T10:15:00Z", "trips": 32, "revenue": 640.0}]
    h = client.get("/v1/demand/hourly", params={"start": "2024-01-05T00:00:00Z", "end": "2024-01-07T00:00:00Z",
                                                 "taxi_type": "yellow"}).json()["data"]
    assert [(x["window_start"], x["trips"], x["avg_fare"]) for x in h] == [
        ("2024-01-05T10:00:00Z", 30, 15.0), ("2024-01-06T10:00:00Z", 10, 8.0)]
    hod = client.get("/v1/demand/by-hour-of-day", params={"taxi_type": "yellow"}).json()["data"]
    assert hod == [{"hour_of_day": 10, "taxi_type": "yellow", "trips": 40, "revenue": 700.0, "days_observed": 2,
                    "avg_trips_per_day": 20.0}]


def test_zones_top_trend_and_filters(client):
    top = client.get("/v1/zones/top", params={**RANGE}).json()["data"]
    assert [(z["zone_id"], z["zone_name"], z["borough"], z["trips"], z["revenue"]) for z in top] == [
        (161, "Midtown Center", "Manhattan", 12, 240.0), (132, "JFK Airport", "Queens", 6, 540.0)]
    q = client.get("/v1/zones/top", params={**RANGE, "borough": "Queens"}).json()["data"]
    assert [z["zone_id"] for z in q] == [132]
    drop = client.get("/v1/zones/top", params={**RANGE, "role": "dropoff"}).json()["data"]
    assert [(z["zone_id"], z["trips"]) for z in drop] == [(161, 2)]
    assert len(client.get("/v1/zones/top", params={**RANGE, "limit": 1}).json()["data"]) == 1
    trend = client.get("/v1/zones/161/trend", params=RANGE).json()["data"]
    assert [(p["window_start"], p["trips"]) for p in trend] == [("2024-01-05T10:00:00Z", 5), ("2024-01-05T10:05:00Z", 7)]


def test_anomaly_endpoints(client):
    c = client.get("/v1/anomalies/counts", params=RANGE).json()["data"]
    assert [(x["anomaly_type"], x["n"]) for x in c] == [("speed_over_80mph", 2), ("tip_over_100", 1)]
    only = client.get("/v1/anomalies/counts", params={**RANGE, "anomaly_type": "tip_over_100"}).json()["data"]
    assert [x["n"] for x in only] == [1]
    d = client.get("/v1/anomalies/demand").json()["data"]
    assert d and d[0]["window_start"] == "2024-01-04T01:05:00Z" and d[0]["trips"] == 200 and d[0]["z_score"] > 3
    assert all(abs(x["z_score"]) > 3 for x in d)


def test_pipeline_status(client):
    s = client.get("/v1/pipeline/status").json()
    by = {q["query_name"]: q for q in s["queries"]}
    tpm = by["gold_trips_per_minute"]
    assert (tpm["last_batch_id"], tpm["total_input_rows"], tpm["total_dropped_groups"], tpm["last_trigger_ms"]) == (2, 1500, 3, 400)
    assert by["gold_hourly_demand"]["total_dropped_groups"] == 1
    assert s["latest_event_window"] == "2024-01-05T11:30:00Z" and s["data_age_seconds"] > 0


def test_empty_database_returns_empty_lists_not_errors(empty_client):
    for path in ["/v1/trips/per-minute", "/v1/demand/sliding", "/v1/demand/hourly", "/v1/zones/top",
                 "/v1/zones/5/trend", "/v1/anomalies/counts", "/v1/anomalies/demand", "/v1/demand/by-hour-of-day"]:
        r = empty_client.get(path)
        assert r.status_code == 200 and r.json()["data"] == [], path
    assert empty_client.get("/v1/pipeline/status").json() == {"latest_event_window": None, "data_age_seconds": None, "queries": []}


# ---- validation & safety --------------------------------------------------------------------------------
@pytest.mark.parametrize("path,params", [
    ("/v1/trips/per-minute", {"start": "2024-01-05T12:00:00Z", "end": "2024-01-05T10:00:00Z"}),
    ("/v1/trips/per-minute", {"start": "2024-01-05T10:00:00Z", "end": "2024-01-05T10:00:00Z"}),
    ("/v1/trips/per-minute", {"limit": 0}), ("/v1/trips/per-minute", {"limit": 20001}),
    ("/v1/trips/per-minute", {"taxi_type": "bus"}), ("/v1/trips/per-minute", {"start": "not-a-date"}),
    ("/v1/zones/top", {"role": "sideways"}), ("/v1/zones/top", {"limit": 301}),
    ("/v1/zones/0/trend", {}), ("/v1/zones/266/trend", {}), ("/v1/zones/abc/trend", {}),
])
def test_invalid_input_is_422(client, path, params):
    assert client.get(path, params=params).status_code == 422


def test_sql_injection_attempts_are_inert(client):
    assert client.get("/v1/trips/per-minute", params={"taxi_type": "yellow' OR '1'='1"}).status_code == 422
    long_payload = "' OR 1=1; DROP TABLE gold.zone_demand_5m; --"          # 49 chars > max_length 40 -> rejected early
    assert client.get("/v1/zones/top", params={**RANGE, "borough": long_payload}).status_code == 422
    short_payload = "x' OR 1=1; DROP TABLE gold.z;--"                       # passes validation -> must stay a literal
    r = client.get("/v1/zones/top", params={**RANGE, "borough": short_payload})
    assert r.status_code == 200 and r.json()["data"] == []
    assert len(client.get("/v1/zones/top", params=RANGE).json()["data"]) == 2   # table intact


def test_api_role_is_read_only():
    conn = psycopg2.connect(host=HOST, port=PORT, user="api_ro", password="api_ro_dev", dbname="mobility_api_test")
    conn.autocommit = True
    with conn.cursor() as cur, pytest.raises(psycopg2.errors.InsufficientPrivilege):
        cur.execute("DELETE FROM gold.trips_per_minute")
    conn.close()


# ---- observability --------------------------------------------------------------------------------------
def test_request_id_is_generated_and_echoed(client):
    assert len(client.get("/health/live").headers["x-request-id"]) == 32
    assert client.get("/health/live", headers={"X-Request-ID": "abc-123"}).headers["x-request-id"] == "abc-123"


def test_metrics_use_route_templates_not_raw_paths(client):
    client.get("/v1/zones/161/trend", params=RANGE)
    client.get("/v1/zones/132/trend", params=RANGE)
    body = client.get("/metrics").text
    assert 'route="/v1/zones/{zone_id}/trend"' in body and "/v1/zones/161/trend" not in body
    assert "api_http_request_duration_seconds_bucket" in body and "api_http_requests_in_flight" in body


def test_access_logs_are_structured(client):
    records = []

    class H(logging.Handler):
        def emit(self, record):
            records.append(record)
    h = H()
    logging.getLogger("api").addHandler(h)
    try:
        client.get("/v1/trips/per-minute", params={**RANGE, "limit": 1}, headers={"X-Request-ID": "log-1"})
    finally:
        logging.getLogger("api").removeHandler(h)
    rec = next(r for r in records if getattr(r, "request_id", None) == "log-1")
    assert (rec.status, rec.route, rec.method) == (200, "/v1/trips/per-minute", "GET") and rec.duration_ms >= 0


# ---- connection-pool behaviour under load -----------------------------------------------------------------
def test_burst_larger_than_pool_queues_instead_of_failing(client):
    from concurrent.futures import ThreadPoolExecutor
    small = ApiSettings(db_host=HOST, db_port=PORT, db_name="mobility_api_test", db_user="api_ro",
                        db_password="api_ro_dev", pool_min=1, pool_max=2, statement_timeout_ms=5000,
                        log_level="INFO", acquire_timeout_s=10.0)
    with TestClient(create_app(small)) as c:
        with ThreadPoolExecutor(max_workers=20) as ex:       # 20 concurrent requests, only 2 connections
            codes = list(ex.map(lambda _: c.get("/v1/trips/per-minute", params=RANGE).status_code, range(60)))
    assert set(codes) == {200}


def test_true_saturation_returns_503_after_bounded_wait(client):
    import threading
    import time
    tight = ApiSettings(db_host=HOST, db_port=PORT, db_name="mobility_api_test", db_user="api_ro",
                        db_password="api_ro_dev", pool_min=1, pool_max=1, statement_timeout_ms=5000,
                        log_level="INFO", acquire_timeout_s=0.3)
    with TestClient(create_app(tight)) as c:
        def hog():
            with c.app.state.db.cursor() as cur:
                cur.execute("SELECT pg_sleep(1.5)")
        th = threading.Thread(target=hog)
        th.start()
        time.sleep(0.3)                                       # the only connection is now busy
        t0 = time.perf_counter()
        r = c.get("/v1/trips/per-minute", params=RANGE)
        waited = time.perf_counter() - t0
        th.join()
        assert r.status_code == 503 and r.headers["retry-after"] == "5" and 0.25 <= waited < 1.2
        assert c.get("/v1/trips/per-minute", params=RANGE).status_code == 200   # recovers once the slot frees
