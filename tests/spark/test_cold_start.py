import pytest

from spark_jobs.common import wait_for_delta_table


def test_wait_sees_a_table_that_is_created_after_an_earlier_miss(spark, tmp_path):
    """Regression (found in the Phase 7 broker drill): Silver polled 'is Bronze there yet?' for minutes before the first
    data arrived, and then NEVER saw the table even though it existed (stuck forever in WAITING_FOR_UPSTREAM)."""
    path = str(tmp_path / "created_later")
    with pytest.raises(TimeoutError):                       # table does not exist yet: a miss (as at cold start)
        wait_for_delta_table(spark, path, poll_seconds=1, timeout_seconds=2)
    spark.range(5).write.format("delta").save(path)         # the upstream job writes its first batch
    wait_for_delta_table(spark, path, poll_seconds=1, timeout_seconds=15)   # must now succeed


def test_wait_returns_immediately_for_an_existing_table(spark, tmp_path):
    path = str(tmp_path / "exists")
    spark.range(3).write.format("delta").save(path)
    wait_for_delta_table(spark, path, poll_seconds=1, timeout_seconds=5)
