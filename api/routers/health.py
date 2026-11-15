from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from api.db import DatabaseUnavailable

router = APIRouter(tags=["health"])
REQUIRED = ["gold.trips_per_minute", "gold.demand_sliding_15m", "gold.zone_demand_5m", "gold.hourly_demand",
            "gold.anomaly_counts", "gold.stream_metrics", "gold.v_demand_by_hour_of_day", "gold.v_demand_anomalies"]


@router.get("/health/live", summary="Liveness: the process is up (never touches the database)")
def live():
    return {"status": "alive"}


@router.get("/health/ready", summary="Readiness: database reachable and serving schema present")
def ready(request: Request):
    checks: dict = {}
    try:
        with request.app.state.db.cursor() as cur:
            cur.execute("SELECT 1")
            checks["database"] = "ok"
            cur.execute("SELECT t, to_regclass(t) IS NOT NULL AS present FROM unnest(%(t)s::text[]) AS t",
                        {"t": REQUIRED})
            missing = [r["t"] for r in cur.fetchall() if not r["present"]]
        checks["schema"] = "ok" if not missing else {"missing": missing}
    except DatabaseUnavailable as e:
        return JSONResponse(status_code=503, content={"status": "not_ready", "checks": {"database": str(e)[:200]}})
    ok = checks["schema"] == "ok"
    return JSONResponse(status_code=200 if ok else 503, content={"status": "ready" if ok else "not_ready",
                                                                 "checks": checks})
