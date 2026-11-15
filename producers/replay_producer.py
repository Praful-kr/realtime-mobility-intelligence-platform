"""Replay TLC parquet files into Kafka as an Avro stream at a configurable speed.

  python -m producers.replay_producer --speed 10 --limit 100000
  python -m producers.replay_producer --speed 0          # unthrottled (benchmark / backfill)

Reliability: idempotent producer, acks=all, bounded in-flight, backpressure on a full local
queue (BufferError -> poll + retry), per-message delivery callbacks, DLQ for rows that cannot
be built/serialized or whose delivery finally fails, SIGINT/SIGTERM flush-and-exit.
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from common.config import load_settings
from prometheus_client import Counter, Gauge, start_http_server

from common.logging_setup import get_logger
from producers.events import COLUMN_MAPS, EventBuildError, ReplayClock, row_to_event

log = get_logger("replay-producer")

P_SENT = Counter("producer_events_sent_total", "Messages handed to Kafka (incl. injected duplicates/poison)")
P_ACKED = Counter("producer_events_acked_total", "Messages acknowledged by all in-sync replicas")
P_FAILED = Counter("producer_events_failed_total", "Messages whose delivery finally failed")
P_DLQ = Counter("producer_dlq_events_total", "Records diverted to the dead-letter topic")
P_BACKPRESSURE = Counter("producer_backpressure_events_total", "Times the local queue was full and produce() had to wait")
P_QUEUE = Gauge("producer_queue_depth", "Messages waiting in the local producer queue")
P_RUNNING = Gauge("producer_running", "1 while a replay is in progress")
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "trip_event_v1.avsc"
UNIFIED = ["pickup_ts", "dropoff_ts", "vendor_id", "passenger_count", "trip_distance", "ratecode_id",
           "store_and_fwd_flag", "pu_location_id", "do_location_id", "payment_type", "fare_amount",
           "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
           "congestion_surcharge", "airport_fee", "trip_type", "ehail_fee"]


def load_unified_table(files: dict[str, Path]) -> pa.Table:
    """Read per-type parquet, rename to unified columns, concat, sort by pickup time."""
    parts = []
    for taxi_type, path in files.items():
        t = pq.read_table(path)
        cmap = COLUMN_MAPS[taxi_type]
        cols = {u: t[src] for src, u in cmap.items() if src in t.column_names}
        n = t.num_rows
        for u in UNIFIED:  # missing columns (schema drift between types/years) become nulls
            if u not in cols:
                cols[u] = pa.nulls(n)
        out = pa.table({u: cols[u] for u in UNIFIED})
        out = out.append_column("taxi_type", pa.array([taxi_type] * n))
        out = out.append_column("source_file", pa.array([path.name] * n))
        out = out.append_column("source_row", pa.array(range(n), pa.int64()))
        parts.append(out)
    merged = pa.concat_tables(parts, promote_options="permissive")
    return merged.take(pc.sort_indices(merged, sort_keys=[("pickup_ts", "ascending")]))


class ReplayProducer:
    def __init__(self, args: argparse.Namespace):
        self.s = load_settings()
        self.args = args
        self.stop = False
        self.sent = self.acked = self.failed = self.dlq = self.backpressure_events = 0
        self._synced: dict = {}
        self.injected_dups = self.injected_poison = self.injected_null_pickup = self.injected_late = 0
        sr = SchemaRegistryClient({"url": self.s.schema_registry_url})
        self.serializer = AvroSerializer(sr, SCHEMA_PATH.read_text(), conf={"auto.register.schemas": True})
        self.producer = Producer({
            "bootstrap.servers": self.s.bootstrap_servers,
            "enable.idempotence": True,          # implies acks=all, retries, max.in.flight<=5
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.size": 131072,
            "queue.buffering.max.messages": args.max_queue,
            "delivery.timeout.ms": 120000,
            "client.id": "replay-producer",
        })

    # -- delivery / DLQ ----------------------------------------------------------------
    def _on_delivery(self, err, msg) -> None:
        if err is None:
            self.acked += 1
            return
        self.failed += 1
        log.error("delivery failed", extra={"error": str(err), "key": (msg.key() or b"").decode()})
        self._to_dlq(msg.key(), "delivery", str(err), {"topic": msg.topic()})

    def _to_dlq(self, key, stage: str, error: str, original) -> None:
        body = json.dumps({"stage": stage, "error": error, "original": original,
                           "ts": datetime.now(timezone.utc).isoformat()}, default=str).encode()
        self._produce_blocking(self.s.dlq_topic, key, body, callback=None)
        self.dlq += 1

    def _produce_blocking(self, topic, key, value, callback) -> None:
        """Backpressure: when librdkafka's local queue is full, drain callbacks and retry."""
        while True:
            try:
                self.producer.produce(topic, key=key, value=value, on_delivery=callback)
                return
            except BufferError:
                self.backpressure_events += 1
                self.producer.poll(0.2)
                if self.stop:
                    raise

    def _sync_metrics(self) -> None:
        """Publish deltas of the plain counters (hot path stays free of Prometheus calls)."""
        last = self._synced
        for counter, now in ((P_SENT, self.sent), (P_ACKED, self.acked), (P_FAILED, self.failed),
                             (P_DLQ, self.dlq), (P_BACKPRESSURE, self.backpressure_events)):
            if now > last.get(counter, 0):
                counter.inc(now - last.get(counter, 0))
                last[counter] = now
        P_QUEUE.set(len(self.producer))

    # -- main loop -----------------------------------------------------------------------
    def run(self) -> int:
        files = {t: Path(self.args.data_dir) / f"{t}_tripdata_{self.args.month}.parquet"
                 for t in self.args.taxi_types}
        table = load_unified_table(files)
        total = min(table.num_rows, self.args.limit) if self.args.limit else table.num_rows
        log.info("replay starting", extra={"rows": total, "speed": self.args.speed, "topic": self.s.raw_topic})
        clock = ReplayClock(self.args.speed, self.args.window_start, self.args.window_end, time.monotonic)
        t0 = last_report = last_sync = time.monotonic()
        P_RUNNING.set(1)
        # pace by wall clock only after the first in-window event, so setup time is excluded
        a = self.args
        deferred: list = []   # (release_pos, original_pos, row) for injected late arrivals
        for offset in range(0, total, 20000):
            if self.stop:
                break
            chunk = table.slice(offset, min(20000, total - offset)).to_pylist()
            for pos, row in enumerate(chunk, start=offset):
                if self.stop:
                    break
                delay = clock.delay_for(row["pickup_ts"]) if row["pickup_ts"] else 0.0
                if delay > 0:
                    time.sleep(delay)
                if a.inject_late_every and (pos + 1) % a.inject_late_every == 0:
                    # late arrival: same content, emitted `late_by_events` positions later (event time unchanged)
                    deferred.append((pos + a.late_by_events, pos, row))
                    self.injected_late += 1
                else:
                    self._emit(row, pos)
                if deferred:
                    due = [d for d in deferred if d[0] <= pos]
                    deferred[:] = [d for d in deferred if d[0] > pos]
                    for _, orig_pos, late_row in due:
                        self._emit(late_row, orig_pos)
                now = time.monotonic()
                if now - last_sync >= 1:
                    self._sync_metrics()
                    last_sync = now
                if now - last_report >= 5:
                    last_report = now
                    log.info("progress", extra={"sent": self.sent, "acked": self.acked, "failed": self.failed,
                                                "dlq": self.dlq, "rate_per_s": round(self.sent / (now - t0)),
                                                "backpressure_events": self.backpressure_events})
        for _, orig_pos, late_row in deferred:   # nothing may be silently lost at end of replay
            self._emit(late_row, orig_pos)
        remaining = self.producer.flush(60)
        self._sync_metrics()
        P_RUNNING.set(0)
        elapsed = time.monotonic() - t0
        log.info("replay finished", extra={"sent": self.sent, "acked": self.acked, "failed": self.failed,
                                           "dlq": self.dlq, "unflushed": remaining, "seconds": round(elapsed, 2),
                                           "avg_rate_per_s": round(self.sent / elapsed) if elapsed else 0,
                                           "backpressure_events": self.backpressure_events,
                                           "injected_dups": self.injected_dups,
                                           "injected_poison": self.injected_poison,
                                           "injected_late": self.injected_late,
                                           "injected_null_pickup": self.injected_null_pickup,
                                           "interrupted": self.stop})
        return 0 if (remaining == 0 and self.failed == 0) else 1

    def _emit(self, row: dict, pos: int) -> None:
        """`pos` = 0-based position in the replay. Fault injection is position-based (never random)
        so tests can predict exactly which events are affected: every Nth event (1-based)."""
        a = self.args
        hit = lambda every: bool(every) and (pos + 1) % every == 0  # noqa: E731
        taxi_type, src_file, src_row = row.pop("taxi_type"), row.pop("source_file"), row.pop("source_row")
        if hit(a.inject_null_pickup_every):
            row["pickup_ts"] = None  # no event time -> must be diverted to the DLQ, not Kafka raw topic
            self.injected_null_pickup += 1
        try:
            event = row_to_event(row, taxi_type, src_file, src_row, datetime.now(timezone.utc))
            value = self.serializer(event, SerializationContext(self.s.raw_topic, MessageField.VALUE))
        except (EventBuildError, ValueError, KeyError) as e:
            from producers.events import make_event_id
            self._to_dlq(make_event_id(taxi_type, src_file, src_row).encode(), "build/serialize", str(e),
                         {"taxi_type": taxi_type, "source_file": src_file, "source_row": src_row})
            self.producer.poll(0)
            return
        key = event["event_id"].encode()
        self._produce_blocking(self.s.raw_topic, key, value, self._on_delivery)
        self.sent += 1
        if hit(a.inject_dup_every):  # at-least-once redelivery: identical bytes, same key
            self._produce_blocking(self.s.raw_topic, key, value, self._on_delivery)
            self.sent += 1
            self.injected_dups += 1
        if hit(a.inject_poison_every):  # undecodable bytes (valid-looking magic byte, garbage body)
            self._produce_blocking(self.s.raw_topic, f"poison-{pos}".encode(),
                                   b"\x00\x00\x00\x00\x01\xff\xfegarbage", self._on_delivery)
            self.sent += 1
            self.injected_poison += 1
        if self.sent % 1000 == 0:
            self.producer.poll(0)


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--speed", type=float, default=1.0, help="1, 10, 100 ... ; 0 = unthrottled")
    p.add_argument("--limit", type=int, default=0, help="max rows (0 = all)")
    p.add_argument("--month", default="2024-01")
    p.add_argument("--taxi-types", type=lambda s: s.split(","), default=["yellow", "green"])
    p.add_argument("--data-dir", default="data/tlc")
    p.add_argument("--window-start", type=datetime.fromisoformat, default=datetime(2024, 1, 1))
    p.add_argument("--window-end", type=datetime.fromisoformat, default=datetime(2024, 2, 1))
    p.add_argument("--max-queue", type=int, default=100000)
    p.add_argument("--metrics-port", type=int, default=8010, help="Prometheus /metrics port (0 = off)")
    p.add_argument("--metrics-linger", type=float, default=12, help="seconds to keep /metrics up after the run")
    p.add_argument("--inject-late-every", type=int, default=0, help="delay every Nth event (late arrival)")
    p.add_argument("--late-by-events", type=int, default=3000, help="how many events later a late event is sent")
    p.add_argument("--inject-dup-every", type=int, default=0, help="re-send every Nth event (duplicate)")
    p.add_argument("--inject-poison-every", type=int, default=0, help="after every Nth event send garbage bytes")
    p.add_argument("--inject-null-pickup-every", type=int, default=0,
                   help="blank pickup time on every Nth event (-> DLQ)")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.metrics_port:
        start_http_server(args.metrics_port)
    rp = ReplayProducer(args)

    def _sig(_signum, _frame):
        log.warning("shutdown requested; flushing")
        rp.stop = True

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)
    try:
        rc = rp.run()
        if args.metrics_port and args.metrics_linger:
            time.sleep(args.metrics_linger)   # let Prometheus scrape the final counter values
        return rc
    except KafkaException as e:
        log.error("kafka error", extra={"error": str(e)})
        return 2


if __name__ == "__main__":
    sys.exit(main())
