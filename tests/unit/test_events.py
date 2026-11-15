import json
import math
from datetime import datetime, timezone
from pathlib import Path

import fastavro
import pytest

from producers.events import (COLUMN_MAPS, EventBuildError, ReplayClock, make_event_id, row_to_event)

ING = datetime(2026, 1, 1, tzinfo=timezone.utc)
SCHEMA = fastavro.parse_schema(json.loads((Path(__file__).parents[2] / "schemas/trip_event_v1.avsc").read_text()))


def good_row(**over):
    row = {"pickup_ts": datetime(2024, 1, 5, 8, 30), "dropoff_ts": datetime(2024, 1, 5, 8, 50),
           "vendor_id": 2, "passenger_count": 1, "trip_distance": 3.2, "fare_amount": 15.5,
           "total_amount": 20.0, "pu_location_id": 161, "do_location_id": 237}
    row.update(over)
    return row


def test_event_id_is_deterministic_and_unique_per_row():
    a = make_event_id("yellow", "f.parquet", 7)
    assert a == make_event_id("yellow", "f.parquet", 7)
    assert a != make_event_id("yellow", "f.parquet", 8)
    assert a != make_event_id("green", "f.parquet", 7)


def test_row_to_event_sets_times_and_ids():
    e = row_to_event(good_row(), "yellow", "f.parquet", 3, ING)
    assert e["event_time"] == e["pickup_ts"] == datetime(2024, 1, 5, 8, 30, tzinfo=timezone.utc)
    assert e["ingestion_time"] == ING and e["event_id"] == make_event_id("yellow", "f.parquet", 3)


def test_missing_pickup_is_rejected():
    with pytest.raises(EventBuildError):
        row_to_event(good_row(pickup_ts=None), "yellow", "f.parquet", 0, ING)


def test_nan_becomes_null_and_int_overflow_rejected():
    e = row_to_event(good_row(trip_distance=math.nan), "yellow", "f.parquet", 0, ING)
    assert e["trip_distance"] is None
    with pytest.raises(EventBuildError):
        row_to_event(good_row(passenger_count=2**40), "yellow", "f.parquet", 0, ING)


def test_negative_fare_is_not_filtered_at_ingestion():
    # Bad-but-well-formed data must reach Bronze untouched; judging it is Silver's job.
    assert row_to_event(good_row(fare_amount=-5.0), "yellow", "f.parquet", 0, ING)["fare_amount"] == -5.0


def test_event_roundtrips_through_the_avro_schema():
    e = row_to_event(good_row(), "green", "g.parquet", 1, ING)
    assert set(e) == {f["name"] for f in SCHEMA["fields"]}
    import io
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, SCHEMA, e)
    buf.seek(0)
    assert fastavro.schemaless_reader(buf, SCHEMA)["event_id"] == e["event_id"]


def test_column_maps_cover_both_taxi_types():
    assert "tpep_pickup_datetime" in COLUMN_MAPS["yellow"] and "lpep_pickup_datetime" in COLUMN_MAPS["green"]


class FakeTime:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


def test_replay_clock_scales_event_time_by_speed():
    ft = FakeTime()
    clock = ReplayClock(10, datetime(2024, 1, 1), datetime(2024, 2, 1), ft)
    t0 = datetime(2024, 1, 1, 0, 0, 0)
    assert clock.delay_for(t0) == 0.0                      # anchor
    assert clock.delay_for(datetime(2024, 1, 1, 0, 1, 40)) == pytest.approx(10.0)  # 100 s / 10x
    ft.t += 4
    assert clock.delay_for(datetime(2024, 1, 1, 0, 1, 40)) == pytest.approx(6.0)   # already waited 4 s


def test_replay_clock_ignores_out_of_window_and_unthrottled():
    clock = ReplayClock(1, datetime(2024, 1, 1), datetime(2024, 2, 1), FakeTime())
    assert clock.delay_for(datetime(2002, 12, 31)) == 0.0
    assert ReplayClock(0, datetime(2024, 1, 1), datetime(2024, 2, 1), FakeTime()).delay_for(datetime(2024, 1, 9)) == 0.0
