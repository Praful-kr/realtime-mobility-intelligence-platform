"""Prometheus instrumentation for Spark streaming drivers.

Why custom lag: Structured Streaming tracks offsets in its own checkpoint and never commits to a Kafka
consumer group, so group-lag exporters show nothing for it. Spark's progress report carries both the
newest available offset and the processed offset per partition, so lag = latest - processed is exact.
For Delta sources the equivalent is "table versions behind".
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from prometheus_client import Counter, Gauge, start_http_server
from pyspark.sql.streaming import StreamingQueryListener

L = ["query"]
INPUT_ROWS = Counter("spark_input_rows_total", "Rows read from the source", L)
BATCHES = Counter("spark_batches_total", "Micro-batches executed", L)
PROCESSED_RPS = Gauge("spark_processed_rows_per_second", "Spark-reported processing rate", L)
INPUT_RPS = Gauge("spark_input_rows_per_second", "Spark-reported arrival rate", L)
BATCH_MS = Gauge("spark_batch_duration_ms", "Trigger execution time of the last micro-batch", L)
STATE_ROWS = Gauge("spark_state_rows", "Rows held in stateful operators", L)
DROPPED = Counter("spark_dropped_by_watermark_groups_total",
                  "Aggregated groups dropped as later than the watermark (lower bound on lost events)", L)
KAFKA_LAG = Gauge("spark_kafka_lag_records", "Planning-time lag: only non-zero when maxOffsetsPerTrigger throttles. "
                  "NOT true consumer lag; use kafka log-end offset - spark_kafka_processed_offset_sum", ["query", "topic"])
KAFKA_PROCESSED = Gauge("spark_kafka_processed_offset_sum", "Sum over partitions of the last processed offset",
                        ["query", "topic"])
DELTA_LAG = Gauge("spark_delta_version_lag", "Delta table versions available but not yet processed", L)
LAST_BATCH_TS = Gauge("spark_last_batch_timestamp_seconds", "Unix time the last micro-batch finished", L)
SILVER_ROWS = Counter("spark_silver_rows_total", "Silver records by outcome", ["outcome"])
SILVER_LATENCY = Gauge("spark_silver_latency_seconds", "Producer emit -> Silver processed, last batch", ["stat"])


def init_silver_series() -> None:
    """Create the Silver outcome series at 0 so dashboards show 0 instead of 'no data'. Called ONLY by the
    Silver job: pre-creating them at import made Bronze and Gold export misleading zero-valued copies."""
    for outcome in ("received", "valid", "quarantined", "undecodable", "duplicate"):
        SILVER_ROWS.labels(outcome)


def kafka_lag(source: Dict[str, Any]) -> Optional[Dict[str, int]]:
    """{topic: lag} from a Kafka source entry of a progress report, or None if it is not a Kafka source."""
    end, latest = source.get("endOffset"), source.get("latestOffset")
    if not isinstance(end, dict) or not isinstance(latest, dict) or "reservoirVersion" in end:
        return None
    out: Dict[str, int] = {}
    for topic, parts in end.items():
        if isinstance(parts, dict) and isinstance(latest.get(topic), dict):
            out[topic] = sum(max(0, latest[topic].get(p, off) - off) for p, off in parts.items())
    return out


def kafka_processed(source: Dict[str, Any]) -> Optional[Dict[str, int]]:
    """{topic: sum of processed (end) offsets over partitions} for a Kafka source entry."""
    end = source.get("endOffset")
    if not isinstance(end, dict) or "reservoirVersion" in end:
        return None
    return {t: sum(parts.values()) for t, parts in end.items() if isinstance(parts, dict)}


def delta_lag(source: Dict[str, Any]) -> Optional[int]:
    end, latest = source.get("endOffset"), source.get("latestOffset")
    if isinstance(end, dict) and "reservoirVersion" in end:
        lv = latest.get("reservoirVersion") if isinstance(latest, dict) else None
        return max(0, (lv if lv is not None else end["reservoirVersion"]) - end["reservoirVersion"])
    return None


def record_progress(p: Dict[str, Any]) -> None:
    q = p.get("name") or "unnamed"
    n = p.get("numInputRows") or 0
    if n:
        INPUT_ROWS.labels(q).inc(n)
    BATCHES.labels(q).inc()
    PROCESSED_RPS.labels(q).set(p.get("processedRowsPerSecond") or 0)
    INPUT_RPS.labels(q).set(p.get("inputRowsPerSecond") or 0)
    BATCH_MS.labels(q).set((p.get("durationMs") or {}).get("triggerExecution") or 0)
    ops = p.get("stateOperators") or []
    STATE_ROWS.labels(q).set(sum(o.get("numRowsTotal", 0) for o in ops))
    dropped = sum(o.get("numRowsDroppedByWatermark", 0) for o in ops)
    if dropped:
        DROPPED.labels(q).inc(dropped)
    for src in p.get("sources") or []:
        for topic, lag in (kafka_lag(src) or {}).items():
            KAFKA_LAG.labels(q, topic).set(lag)
        for topic, done in (kafka_processed(src) or {}).items():
            KAFKA_PROCESSED.labels(q, topic).set(done)
        dl = delta_lag(src)
        if dl is not None:
            DELTA_LAG.labels(q).set(dl)
    import time
    LAST_BATCH_TS.labels(q).set(time.time())


def record_silver_batch(m: Dict[str, Any]) -> None:
    SILVER_ROWS.labels("received").inc(m["bronze_rows"])
    SILVER_ROWS.labels("valid").inc(m["silver_inserted"])
    SILVER_ROWS.labels("quarantined").inc(m["quarantined"])
    SILVER_ROWS.labels("undecodable").inc(m["undecodable"])
    SILVER_ROWS.labels("duplicate").inc(m["dup_in_batch"] + m["dup_vs_silver"])
    if m.get("latency_avg_s") is not None:
        SILVER_LATENCY.labels("avg").set(m["latency_avg_s"])
        SILVER_LATENCY.labels("max").set(m["latency_max_s"])


class PrometheusListener(StreamingQueryListener):
    def onQueryStarted(self, event):  # noqa: N802
        pass

    def onQueryProgress(self, event):  # noqa: N802
        try:
            record_progress(json.loads(event.progress.json))
        except Exception as e:  # observability must never break the stream
            print("METRICS_ERROR " + repr(e), flush=True)

    def onQueryTerminated(self, event):  # noqa: N802
        pass


def start_metrics(spark) -> Optional[PrometheusListener]:
    """Serve /metrics on METRICS_PORT (if set) and attach the progress listener."""
    port = int(os.getenv("METRICS_PORT", "0"))
    if not port:
        return None
    start_http_server(port)
    listener = PrometheusListener()
    spark.streams.addListener(listener)
    return listener
