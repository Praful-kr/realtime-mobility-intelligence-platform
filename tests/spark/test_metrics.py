from prometheus_client import REGISTRY, generate_latest

from spark_jobs import metrics


def exposed():
    return generate_latest(REGISTRY).decode()


def test_importing_metrics_does_not_create_silver_series():
    assert "spark_silver_rows_total{" not in exposed()      # Bronze/Gold import this module too


def test_init_silver_series_creates_all_outcomes_at_zero_and_silver_batch_counts():
    metrics.init_silver_series()
    text = exposed()
    for o in ("received", "valid", "quarantined", "undecodable", "duplicate"):
        assert f'spark_silver_rows_total{{outcome="{o}"}} 0.0' in text
    metrics.record_silver_batch({"bronze_rows": 10, "silver_inserted": 6, "quarantined": 3, "undecodable": 1,
                                 "dup_in_batch": 1, "dup_vs_silver": 0, "latency_avg_s": 1.5, "latency_max_s": 4.0})
    text = exposed()
    assert 'spark_silver_rows_total{outcome="received"} 10.0' in text and 'outcome="duplicate"} 1.0' in text
    assert 'spark_silver_latency_seconds{stat="max"} 4.0' in text


def test_kafka_lag_is_latest_minus_processed_per_topic():
    src = {"endOffset": {"t": {"0": 90, "1": 50}}, "latestOffset": {"t": {"0": 100, "1": 50}}}
    assert metrics.kafka_lag(src) == {"t": 10} and metrics.delta_lag(src) is None


def test_delta_lag_is_versions_behind_and_not_mistaken_for_kafka():
    src = {"endOffset": {"reservoirVersion": 5, "index": -1}, "latestOffset": {"reservoirVersion": 8, "index": -1}}
    assert metrics.delta_lag(src) == 3 and metrics.kafka_lag(src) is None


def test_record_progress_exports_rates_state_drops_and_lag():
    metrics.record_progress({"name": "q-test", "numInputRows": 100, "processedRowsPerSecond": 50.0,
                             "inputRowsPerSecond": 60.0, "durationMs": {"triggerExecution": 250},
                             "stateOperators": [{"numRowsTotal": 7, "numRowsDroppedByWatermark": 2}],
                             "sources": [{"endOffset": {"trips": {"0": 5}}, "latestOffset": {"trips": {"0": 9}}}]})
    t = exposed()
    for line in ('spark_input_rows_total{query="q-test"} 100.0', 'spark_processed_rows_per_second{query="q-test"} 50.0',
                 'spark_batch_duration_ms{query="q-test"} 250.0', 'spark_state_rows{query="q-test"} 7.0',
                 'spark_dropped_by_watermark_groups_total{query="q-test"} 2.0',
                 'spark_kafka_lag_records{query="q-test",topic="trips"} 4.0'):
        assert line in t, line


def test_processed_offset_sum_is_exported_so_true_lag_can_be_computed_in_prometheus():
    src = {"endOffset": {"trips": {"0": 5, "1": 7}}, "latestOffset": {"trips": {"0": 5, "1": 7}}}
    assert metrics.kafka_processed(src) == {"trips": 12}
    assert metrics.kafka_processed({"endOffset": {"reservoirVersion": 3}}) is None
    metrics.record_progress({"name": "q-lag", "sources": [src]})
    assert 'spark_kafka_processed_offset_sum{query="q-lag",topic="trips"} 12.0' in exposed()
