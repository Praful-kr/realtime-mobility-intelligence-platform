"""Read-only analytics endpoints over the Gold serving tables. SQL is parameterized; table names come from a
fixed whitelist; every list endpoint is bounded by `limit`."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Optional

from fastapi import APIRouter, Path, Query, Request

from api.schemas import (AnomalyCount, DemandAnomaly, Envelope, HourlyDemand, HourOfDay, Meta, PipelineStatus,
                         QueryStatus, Role, SlidingDemand, Start, End, TaxiType, TripsPerMinute, ZonePoint,
                         ZoneTotal, resolve_range)

router = APIRouter(prefix="/v1")
Limit = Annotated[int, Query(ge=1, le=20000, description="Maximum rows returned")]


def _val(e):  # Enum | None -> str | None
    return e.value if e is not None else None


def _env(rows, rng, limit):
    return {"data": rows, "meta": Meta(start=rng[0] if rng else None, end=rng[1] if rng else None,
                                       count=len(rows), limit=limit)}


@router.get("/trips/per-minute", response_model=Envelope[TripsPerMinute], tags=["trips"],
            summary="Trips, revenue and averages per 1-minute tumbling window")
def trips_per_minute(request: Request, start: Start = None, end: End = None, taxi_type: Optional[TaxiType] = None,
                     limit: Limit = 1000):
    db = request.app.state.db
    rng = resolve_range(start, end, db.latest("minute"), default_hours=1)
    if rng is None:
        return _env([], None, limit)
    rows = db.query("""SELECT window_start, taxi_type, trips, revenue, avg_fare, avg_duration_s, tip_sum, distance_sum
                       FROM gold.trips_per_minute
                       WHERE window_start >= %(s)s AND window_start < %(e)s
                         AND (%(tt)s::text IS NULL OR taxi_type = %(tt)s)
                       ORDER BY window_start, taxi_type LIMIT %(lim)s""",
                    {"s": rng[0], "e": rng[1], "tt": _val(taxi_type), "lim": limit})
    return _env(rows, rng, limit)


@router.get("/demand/sliding", response_model=Envelope[SlidingDemand], tags=["demand"],
            summary="Rolling demand: 15-minute windows sliding every 5 minutes")
def demand_sliding(request: Request, start: Start = None, end: End = None, limit: Limit = 1000):
    db = request.app.state.db
    rng = resolve_range(start, end, db.latest("sliding"), default_hours=6)
    if rng is None:
        return _env([], None, limit)
    rows = db.query("""SELECT window_start, window_end, trips, revenue FROM gold.demand_sliding_15m
                       WHERE window_start >= %(s)s AND window_start < %(e)s
                       ORDER BY window_start LIMIT %(lim)s""", {"s": rng[0], "e": rng[1], "lim": limit})
    return _env(rows, rng, limit)


@router.get("/demand/hourly", response_model=Envelope[HourlyDemand], tags=["demand"],
            summary="Demand per hourly tumbling window")
def demand_hourly(request: Request, start: Start = None, end: End = None, taxi_type: Optional[TaxiType] = None,
                  limit: Limit = 1000):
    db = request.app.state.db
    rng = resolve_range(start, end, db.latest("hourly"), default_hours=24)
    if rng is None:
        return _env([], None, limit)
    rows = db.query("""SELECT window_start, taxi_type, trips, revenue,
                              CASE WHEN trips > 0 THEN fare_sum / trips END AS avg_fare
                       FROM gold.hourly_demand
                       WHERE window_start >= %(s)s AND window_start < %(e)s
                         AND (%(tt)s::text IS NULL OR taxi_type = %(tt)s)
                       ORDER BY window_start, taxi_type LIMIT %(lim)s""",
                    {"s": rng[0], "e": rng[1], "tt": _val(taxi_type), "lim": limit})
    return _env(rows, rng, limit)


@router.get("/demand/by-hour-of-day", response_model=Envelope[HourOfDay], tags=["demand"],
            summary="Demand profile by hour of day (all observed days)")
def demand_by_hour_of_day(request: Request, taxi_type: Optional[TaxiType] = None):
    rows = request.app.state.db.query(
        """SELECT hour_of_day, taxi_type, trips, revenue, days_observed FROM gold.v_demand_by_hour_of_day
           WHERE (%(tt)s::text IS NULL OR taxi_type = %(tt)s) ORDER BY hour_of_day, taxi_type""",
        {"tt": _val(taxi_type)})
    for r in rows:
        r["avg_trips_per_day"] = r["trips"] / r["days_observed"] if r["days_observed"] else 0.0
    return _env(rows, None, None)


@router.get("/zones/top", response_model=Envelope[ZoneTotal], tags=["zones"],
            summary="Most popular pickup or drop-off zones in a time range")
def zones_top(request: Request, role: Role = Role.pickup, start: Start = None, end: End = None,
              borough: Optional[Annotated[str, Query(max_length=40)]] = None,
              limit: Annotated[int, Query(ge=1, le=300)] = 10):
    db = request.app.state.db
    rng = resolve_range(start, end, db.latest("zone"), default_hours=1)
    if rng is None:
        return _env([], None, limit)
    rows = db.query("""SELECT zone_id, max(zone_name) AS zone_name, max(borough) AS borough,
                              sum(trips)::bigint AS trips, sum(revenue) AS revenue
                       FROM gold.zone_demand_5m
                       WHERE role = %(role)s AND window_start >= %(s)s AND window_start < %(e)s
                         AND (%(b)s::text IS NULL OR borough = %(b)s)
                       GROUP BY zone_id ORDER BY trips DESC, zone_id LIMIT %(lim)s""",
                    {"role": role.value, "s": rng[0], "e": rng[1], "b": borough, "lim": limit})
    return _env(rows, rng, limit)


@router.get("/zones/{zone_id}/trend", response_model=Envelope[ZonePoint], tags=["zones"],
            summary="5-minute demand trend for one zone")
def zone_trend(request: Request, zone_id: Annotated[int, Path(ge=1, le=265)], role: Role = Role.pickup,
               start: Start = None, end: End = None, limit: Limit = 1000):
    db = request.app.state.db
    rng = resolve_range(start, end, db.latest("zone"), default_hours=6)
    if rng is None:
        return _env([], None, limit)
    rows = db.query("""SELECT window_start, trips, revenue FROM gold.zone_demand_5m
                       WHERE zone_id = %(z)s AND role = %(role)s AND window_start >= %(s)s AND window_start < %(e)s
                       ORDER BY window_start LIMIT %(lim)s""",
                    {"z": zone_id, "role": role.value, "s": rng[0], "e": rng[1], "lim": limit})
    return _env(rows, rng, limit)


@router.get("/anomalies/counts", response_model=Envelope[AnomalyCount], tags=["anomalies"],
            summary="Trip-level outlier counts per minute")
def anomaly_counts(request: Request, start: Start = None, end: End = None,
                   anomaly_type: Optional[Annotated[str, Query(max_length=40)]] = None, limit: Limit = 1000):
    db = request.app.state.db
    rng = resolve_range(start, end, db.latest("anomaly"), default_hours=6)
    if rng is None:
        return _env([], None, limit)
    rows = db.query("""SELECT window_start, anomaly_type, n FROM gold.anomaly_counts
                       WHERE window_start >= %(s)s AND window_start < %(e)s
                         AND (%(t)s::text IS NULL OR anomaly_type = %(t)s)
                       ORDER BY window_start, anomaly_type LIMIT %(lim)s""",
                    {"s": rng[0], "e": rng[1], "t": anomaly_type, "lim": limit})
    return _env(rows, rng, limit)


@router.get("/anomalies/demand", response_model=Envelope[DemandAnomaly], tags=["anomalies"],
            summary="Minutes whose demand deviates > 3 sigma from the trailing hour")
def demand_anomalies(request: Request, start: Start = None, end: End = None,
                     limit: Annotated[int, Query(ge=1, le=1000)] = 100):
    db = request.app.state.db
    rng = resolve_range(start, end, db.latest("minute"), default_hours=24 * 365 * 10)
    if rng is None:
        return _env([], None, limit)
    rows = db.query("""SELECT window_start, trips, trailing_mean, trailing_sd, z_score FROM gold.v_demand_anomalies
                       WHERE window_start >= %(s)s AND window_start < %(e)s
                       ORDER BY abs(z_score) DESC LIMIT %(lim)s""", {"s": rng[0], "e": rng[1], "lim": limit})
    return _env(rows, rng, limit)


@router.get("/pipeline/status", response_model=PipelineStatus, tags=["pipeline"],
            summary="Streaming job progress and data freshness")
def pipeline_status(request: Request):
    db = request.app.state.db
    rows = db.query("""WITH last AS (
                         SELECT DISTINCT ON (query_name) query_name, batch_id, ts, trigger_ms, processed_rows_per_s
                         FROM gold.stream_metrics ORDER BY query_name, ts DESC),
                       tot AS (SELECT query_name, sum(input_rows)::bigint AS inp, sum(dropped_by_watermark)::bigint AS dr
                               FROM gold.stream_metrics GROUP BY 1)
                       SELECT l.query_name, l.batch_id AS last_batch_id, l.ts AS last_seen,
                              COALESCE(t.inp, 0) AS total_input_rows, COALESCE(t.dr, 0) AS total_dropped_groups,
                              l.trigger_ms AS last_trigger_ms, l.processed_rows_per_s AS last_rows_per_s
                       FROM last l JOIN tot t USING (query_name) ORDER BY l.query_name""", {})
    latest = db.latest("minute")
    age = (datetime.now(timezone.utc) - latest).total_seconds() if latest else None
    return PipelineStatus(latest_event_window=latest, data_age_seconds=age,
                          queries=[QueryStatus(**r) for r in rows])
