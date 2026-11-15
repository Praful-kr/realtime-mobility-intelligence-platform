from datetime import datetime

import pytest

from spark_jobs.dq_rules import ERROR_RULES, RULE_SPECS, WARN_RULES, apply_rules, decode_bronze
from tests.spark.conftest import SCHEMA_JSON, UTC, bronze_df, encode, good_event


def run(spark, payloads):
    return {r["kafka_offset"]: r for r in apply_rules(decode_bronze(bronze_df(spark, payloads), SCHEMA_JSON)).collect()}


def test_clean_event_has_no_findings_and_full_score(spark):
    r = run(spark, [encode(good_event())])[0]
    assert r["errors"] == [] and r["warnings"] == [] and r["dq_score"] == 1.0
    assert r["event_id"] == "evt-1" and r["trip_duration_seconds"] == 1200


ERROR_CASES = {
    "negative_fare": dict(fare_amount=-5.0),
    "negative_total": dict(total_amount=-1.0),
    "negative_distance": dict(trip_distance=-0.5),
    "null_fare": dict(fare_amount=None),
    "implausible_distance": dict(trip_distance=999.0),
    "implausible_fare": dict(fare_amount=5000.0),
    "invalid_pickup_zone": dict(pu_location_id=0),
    "invalid_dropoff_zone": dict(do_location_id=300),
    "invalid_passenger_count": dict(passenger_count=12),
    "invalid_taxi_type": dict(taxi_type="uber"),
    "dropoff_before_pickup": dict(dropoff_ts=datetime(2024, 1, 5, 8, 0, tzinfo=UTC)),
    "trip_too_long": dict(dropoff_ts=datetime(2024, 1, 7, 8, 30, tzinfo=UTC)),
    "pickup_out_of_range": dict(event_time=datetime(2002, 12, 31, tzinfo=UTC), pickup_ts=datetime(2002, 12, 31, tzinfo=UTC),
                                dropoff_ts=datetime(2002, 12, 31, 1, tzinfo=UTC)),
}


@pytest.mark.parametrize("rule,override", ERROR_CASES.items())
def test_each_error_rule_fires_and_zeroes_score(spark, rule, override):
    r = run(spark, [encode(good_event(**override))])[0]
    assert rule in r["errors"], r["errors"]
    assert r["dq_score"] == 0.0


def test_future_pickup_is_an_error(spark):
    future = datetime(2026, 6, 1, tzinfo=UTC)  # ingestion_time in the fixture is 2026-01-01
    r = run(spark, [encode(good_event(event_time=future, pickup_ts=future, dropoff_ts=None))])[0]
    assert "pickup_out_of_range" in r["errors"]


WARN_CASES = {
    "missing_dropoff": dict(dropoff_ts=None),
    "null_passenger_count": dict(passenger_count=None),
    "zero_passenger_count": dict(passenger_count=0),
    "zero_distance": dict(trip_distance=0.0),
    "null_payment_type": dict(payment_type=None),
    "total_less_than_fare": dict(total_amount=10.0),
}


@pytest.mark.parametrize("rule,override", WARN_CASES.items())
def test_each_warn_rule_keeps_record_and_reduces_score(spark, rule, override):
    r = run(spark, [encode(good_event(**override))])[0]
    assert r["errors"] == [] and rule in r["warnings"]
    assert r["dq_score"] == pytest.approx(1.0 - 0.1 * len(r["warnings"]))


def test_every_catalog_rule_has_a_test():
    assert set(ERROR_CASES) | {"undecodable_payload"} == set(ERROR_RULES)
    assert set(WARN_CASES) == set(WARN_RULES)
    assert len(RULE_SPECS) == len(ERROR_RULES) + len(WARN_RULES)


def test_garbage_wrong_magic_null_and_truncated_payloads_are_undecodable(spark):
    good = encode(good_event())
    rows = run(spark, [b"\x00\x00\x00\x00\x01\xff\xfegarbage", b"\x05" + good[1:], None, good[:12], b""])
    for off, r in rows.items():
        assert r["errors"] == ["undecodable_payload"], (off, r["errors"])
        assert r["dq_score"] == 0.0


def test_multiple_errors_are_all_reported(spark):
    r = run(spark, [encode(good_event(fare_amount=-1.0, trip_distance=-1.0))])[0]
    assert {"negative_fare", "negative_distance"} <= set(r["errors"])
