"""Pure functions: TLC row -> TripEvent dict, deterministic ids, replay pacing. No I/O."""
from __future__ import annotations

import math
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

_NS = uuid.UUID("6f1d0c2e-8a64-4f5e-9a37-2b7c1d4e5f60")
INT32_MIN, INT32_MAX = -(2**31), 2**31 - 1

# source column -> unified column, per taxi type (pickup/dropoff names differ).
_COMMON = {
    "VendorID": "vendor_id", "passenger_count": "passenger_count", "trip_distance": "trip_distance",
    "RatecodeID": "ratecode_id", "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id", "DOLocationID": "do_location_id", "payment_type": "payment_type",
    "fare_amount": "fare_amount", "extra": "extra", "mta_tax": "mta_tax", "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount", "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount", "congestion_surcharge": "congestion_surcharge",
}
COLUMN_MAPS: dict[str, dict[str, str]] = {
    "yellow": {**_COMMON, "tpep_pickup_datetime": "pickup_ts", "tpep_dropoff_datetime": "dropoff_ts",
               "Airport_fee": "airport_fee"},
    "green": {**_COMMON, "lpep_pickup_datetime": "pickup_ts", "lpep_dropoff_datetime": "dropoff_ts",
              "trip_type": "trip_type", "ehail_fee": "ehail_fee"},
}
INT_FIELDS = {"vendor_id", "passenger_count", "ratecode_id", "pu_location_id", "do_location_id",
              "payment_type", "trip_type"}
DOUBLE_FIELDS = {"trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                 "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee", "ehail_fee"}
OPTIONAL_FIELDS = INT_FIELDS | DOUBLE_FIELDS | {"store_and_fwd_flag", "dropoff_ts"}


class EventBuildError(ValueError):
    """Row cannot be turned into a valid TripEvent (goes to the DLQ)."""


def make_event_id(taxi_type: str, source_file: str, source_row: int) -> str:
    return str(uuid.uuid5(_NS, f"{taxi_type}|{source_file}|{source_row}"))


def _as_utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def _clean(field: str, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if field in INT_FIELDS:
        v = int(value)
        if not INT32_MIN <= v <= INT32_MAX:
            raise EventBuildError(f"{field} out of int32 range: {value}")
        return v
    if field in DOUBLE_FIELDS:
        v = float(value)
        if math.isinf(v):
            raise EventBuildError(f"{field} is infinite")
        return v
    return value


def row_to_event(row: dict[str, Any], taxi_type: str, source_file: str, source_row: int,
                 ingestion_time: datetime) -> dict[str, Any]:
    """`row` already uses unified column names. Pickup time is the only hard requirement:
    without it there is no event time, so the record cannot enter the stream (-> DLQ)."""
    pickup = row.get("pickup_ts")
    if pickup is None:
        raise EventBuildError("missing pickup timestamp")
    event: dict[str, Any] = {
        "event_id": make_event_id(taxi_type, source_file, source_row),
        "taxi_type": taxi_type,
        "event_time": _as_utc(pickup),
        "ingestion_time": _as_utc(ingestion_time),
        "source_file": source_file,
        "source_row": int(source_row),
        "pickup_ts": _as_utc(pickup),
    }
    for field in OPTIONAL_FIELDS:
        value = _clean(field, row.get(field))
        event[field] = _as_utc(value) if field == "dropoff_ts" and value is not None else value
    return event


class ReplayClock:
    """Maps event time to wall time at `speed`x. Events outside [window_start, window_end) are
    anomalies (e.g. 2002 timestamps) and are emitted immediately, without distorting the pacing.
    speed <= 0 means unthrottled. Clock is injectable for deterministic tests."""

    def __init__(self, speed: float, window_start: datetime, window_end: datetime,
                 now: Callable[[], float]):
        self.speed, self.start, self.end, self._now = speed, window_start, window_end, now
        self._anchor_event: datetime | None = None
        self._anchor_wall = 0.0

    def delay_for(self, event_time: datetime) -> float:
        if self.speed <= 0 or not (self.start <= event_time < self.end):
            return 0.0
        if self._anchor_event is None:
            self._anchor_event, self._anchor_wall = event_time, self._now()
        target = self._anchor_wall + (event_time - self._anchor_event).total_seconds() / self.speed
        return max(0.0, target - self._now())
