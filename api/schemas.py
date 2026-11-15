"""Response models and shared query-parameter validation."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Annotated, Generic, List, Optional, TypeVar

from fastapi import HTTPException, Query
from pydantic import BaseModel, field_validator

T = TypeVar("T")


class _Base(BaseModel):
    """Floats are rounded to 4 dp at the API boundary: sums of doubles carry representation noise
    (442936.13999999984). Stored values are untouched; money is exact to the cent."""

    @field_validator("*", mode="after")
    @classmethod
    def _round(cls, v):
        return round(v, 4) if isinstance(v, float) else v


class TaxiType(str, Enum):
    yellow = "yellow"
    green = "green"


class Role(str, Enum):
    pickup = "pickup"
    dropoff = "dropoff"


class Meta(BaseModel):
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    count: int
    limit: Optional[int] = None


class Envelope(BaseModel, Generic[T]):
    data: List[T]
    meta: Meta


class TripsPerMinute(_Base):
    window_start: datetime
    taxi_type: str
    trips: int
    revenue: Optional[float]
    avg_fare: Optional[float]
    avg_duration_s: Optional[float]
    tip_sum: Optional[float]
    distance_sum: Optional[float]


class SlidingDemand(_Base):
    window_start: datetime
    window_end: datetime
    trips: int
    revenue: Optional[float]


class HourlyDemand(_Base):
    window_start: datetime
    taxi_type: str
    trips: int
    revenue: Optional[float]
    avg_fare: Optional[float]


class HourOfDay(_Base):
    hour_of_day: int
    taxi_type: str
    trips: int
    revenue: Optional[float]
    days_observed: int
    avg_trips_per_day: float


class ZoneTotal(_Base):
    zone_id: int
    zone_name: Optional[str]
    borough: Optional[str]
    trips: int
    revenue: Optional[float]


class ZonePoint(_Base):
    window_start: datetime
    trips: int
    revenue: Optional[float]


class AnomalyCount(_Base):
    window_start: datetime
    anomaly_type: str
    n: int


class DemandAnomaly(_Base):
    window_start: datetime
    trips: int
    trailing_mean: float
    trailing_sd: float
    z_score: float


class QueryStatus(_Base):
    query_name: str
    last_batch_id: Optional[int]
    last_seen: datetime
    total_input_rows: int
    total_dropped_groups: int
    last_trigger_ms: Optional[int]
    last_rows_per_s: Optional[float]


class PipelineStatus(_Base):
    latest_event_window: Optional[datetime]
    data_age_seconds: Optional[float]
    queries: List[QueryStatus]


# ---- shared query params ---------------------------------------------------------------------------
Start = Annotated[Optional[datetime], Query(description="Inclusive window start (ISO-8601; naive = UTC)")]
End = Annotated[Optional[datetime], Query(description="Exclusive window end (ISO-8601; naive = UTC)")]


def _utc(d: Optional[datetime]) -> Optional[datetime]:
    return None if d is None else (d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc))


def resolve_range(start: Optional[datetime], end: Optional[datetime], latest: Optional[datetime],
                  default_hours: float):
    """Defaults are anchored on the newest data (historical replay), never wall-clock. Returns None when the
    table is empty and no explicit range was given."""
    start, end = _utc(start), _utc(end)
    if start and end and start >= end:
        raise HTTPException(status_code=422, detail="start must be earlier than end")
    if end is None:
        if latest is None:
            return None
        end = latest + timedelta(seconds=1)
    if start is None:
        start = end - timedelta(hours=default_hours)
    return start, end
