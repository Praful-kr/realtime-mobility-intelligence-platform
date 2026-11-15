# Reliability drills

Each drill injects a real fault into the running stack, then checks the outcome against **independent oracles**: plain-Python re-implementations that read the raw TLC parquet and share no code with the Spark jobs (`tests/e2e/silver_oracle.py`, `tests/e2e/gold_oracle.py`). A drill passes only if every check passes. Raw evidence is written to `data/drills/<name>.json` (git-ignored; regenerate with `make drill-<name>`).

Run: `make drill-coldstart | drill-producer | drill-graceful | drill-crash | drill-broker | drill-sigkill`. They **reset the lake and Kafka topics**, so use them on local data only.

**Guarantee being tested:** at-least-once delivery from Kafka plus idempotent sinks gives *effectively-once per sink*. There is no transaction across Kafka, Delta and Postgres, so end-to-end exactly-once is not claimed.

## Summary

| Drill | Fault | Result |
|---|---|---|
| producer | Tiny local queue + unthrottled send; SIGTERM mid-replay | Pass |
| graceful | `docker stop` Silver while a batch is running | Pass, 9/9 |
| crash | Process dies after the Delta write, before the offset commit | Pass, 6/6 (with a control run that fails as it should) |
| coldstart | Downstream job started before its upstream table exists | Pass |
| broker | Stop `kafka-2` for ~45 s under load | Pass, 10/10 |
| sigkill | JVM of Bronze, Silver and Gold killed in turn under load | Pass, 7/7 |

Setup for the broker and kill drills: 100,000 TLC rows replayed at ~815 events/s with injected faults (every 50th event duplicated, garbage bytes after every 997th, a null pickup time every 1009th). Expected: Bronze 102,000 rows, Silver 98,351, quarantine 1,674, 1,975 duplicates dropped.

## 1. Producer: backpressure and SIGTERM

- 60,000 rows through a 500-message local queue at full speed: **sent = acked = 60,000, failed 0, 276 backpressure events**, Kafka end offsets = 60,000. The producer slows down when the queue fills instead of dropping or crashing.
- SIGTERM mid-run: the producer flushes, exits 0, and Kafka end offsets equal what it reported sent (3,886).

## 2. Graceful shutdown

Finding: `spark-submit` as PID 1 meant Python's SIGTERM handlers never ran, so `docker stop` ended with **exit 143** and no clean stop. Fix: `submit.sh` now `exec`s the Python driver directly, and `run_until_stopped` waits for the in-flight micro-batch before stopping queries; compose sets `stop_grace_period: 60s`.

Result on a 200,000-row run: Silver stopped with **exit 0**, the log shows `SHUTDOWN_REQUESTED` before the batch finished and `SHUTDOWN_COMPLETE` after, and Silver and Gold still match the oracles exactly (197,075 rows). Idle stops of Bronze/Silver/Gold/API take 0.5–1.6 s.

## 3. Crash after the sink write (idempotence, with a negative control)

The hardest case for effectively-once: the batch is written to Delta, then the process dies before Spark records the offsets. On restart Spark replays the same batch.

`CHAOS_CRASH_AFTER_WRITE=bronze:1` kills the process (`os._exit(137)`) right after batch 1 is written. A marker file stops the chaos from firing again on the restart.

| Run | Bronze rows | Kafka rows | Duplicates |
|---|---|---|---|
| Idempotent writes (`txnAppId` + `txnVersion = batchId`) | 100,000 | 100,000 | none; commit sizes sum to exactly 100,000 |
| **Control**: idempotence disabled | 119,997 | 100,000 | 19,997, which is exactly one replayed micro-batch (that size appears twice in Delta's commit history) |

The control matters: it shows the check can detect the failure, so the pass is not vacuous.

Limit: this protection depends on the checkpoint. If a checkpoint is deleted but the table is kept, the restarted job re-appends from the beginning.

## 4. Cold start

Silver and Gold failed with `DELTA_SCHEMA_NOT_SET` when they started before Bronze had written. The first fix (poll for the table) still hung forever when the table was created by *another process*, because the waiting JVM kept a stale "table does not exist" view. `wait_for_delta_table` now checks `_delta_log` through the Hadoop filesystem and clears Delta's cache (`DeltaLog.clearCache()`, `refreshByPath`) before reading. A regression test and the drill cover it: after 25 misses the waiter found the table 56 s later. Gold also now starts its metrics endpoint *before* waiting, so Prometheus does not report it as down.

## 5. Kafka broker failure under load

`kafka-2` stopped at t+25 s and restarted at t+70 s while the replay ran.

| Check | Observed |
|---|---|
| Detection | Exporter saw `kafka_brokers < 3` after 14 s; partitions under-replicated |
| Producer | 0 failed sends, sent = acked (acks=all, RF=3, min ISR 2 tolerates one broker down) |
| Recovery | Broker back and all partitions in sync **28.5 s** after restart |
| Alerts | `KafkaBrokerMissing`, `KafkaUnderReplicatedPartitions` and `DeadLetterQueueGrowing` fired; **`TargetDown` did not** (the Spark jobs were not disturbed) |
| Data | Kafka raw end offsets = producer sent; Silver and Gold match both oracles exactly |
| Lag | Peak true lag 26,871 records, drained to 0; all three drivers then stopped with exit 0 |

## 6. Hard kill of each Spark job under load

At t+20 s, 70 s and 120 s of the same 100k-event replay, the JVM inside `spark-bronze`, `spark-silver` and `spark-gold` is killed with SIGKILL (the equivalent of an out-of-memory kill). Nothing restarts them except the compose policy `restart: on-failure`.

| Check | Observed |
|---|---|
| Restart | Compose restarted all three itself. A new process was serving metrics after **12.4 s** (Bronze), **17.0 s** (Silver), **11.7 s** (Gold) |
| Producer | Unaffected: 102,000 sent = acked, 0 failed |
| Convergence | Postgres Gold total reached 98,351 (the expected value) after the three crashes |
| Silver | Matches the oracle exactly: 98,351 rows, 1,674 quarantined, 1,975 duplicates dropped, Bronze 102,000. No loss, no duplicates |
| Gold | Matches both the batch recompute and the independent oracle |
| Lag | Peak true lag 68,497 records, drained to 0; all three drivers then stopped with exit 0 |

Each restarted driver resumes from its checkpoint, and the idempotent sinks make the replayed batches harmless. Recovery times come from one run each.

The `TargetDown` alert did appear in the evidence of this drill's alert list, but that run's lookback included the Spark jobs' startup, so I do not claim anything about alerts for this drill.

## Bugs and wrong assumptions these drills exposed

1. **Exit 143 on `docker stop`**: handlers never ran with `spark-submit` as PID 1 (fixed, see 2).
2. **Cross-process stale view of a missing Delta table** (fixed, see 4).
3. **Harness bug:** `prom(x) or 1` replaced a legitimate `0.0` with 1 (a falsy zero). The broker recovery check now compares explicitly.
4. **A wrong check:** I first asserted "exactly 20,000 duplicates" for the control run. Rate limiting makes batch sizes vary, so it now asserts "exactly one replayed batch" from the commit history.
5. **Verifier starved the pipeline:** running `verify-silver` (a fourth 2 GB JVM) beside three live drivers timed out on the 8 GB VM, twice. The pipeline itself was fine (Silver had received all 102,000 messages, no restarts, no OOM). The drills now stop the drivers, gracefully, before verifying.
6. **`docker kill` is not a crash:** Docker never applies a restart policy to a manual kill (checked with a throw-away container: `kill` left it stopped, a process that exits by itself was restarted three times). My first SIGKILL drill therefore had to restart services by hand, and it said so (`auto-restart: False`). It now kills the JVM *inside* the container, which behaves like an out-of-memory kill.
7. **Recovery time was measured wrongly, and so were the peak lag and alerts:** "target up" read Prometheus's last pre-kill scrape, so it reported 1.6 s. It now requires a new process (`process_start_time_seconds` later than the kill). Peak lag and fired alerts were also read over a fixed 40-minute window that included earlier drills (two runs reported an identical 94,316) and then over a window with a 15 s margin that caught the Spark jobs' startup (a false `TargetDown`). They now cover exactly the drill's own duration.
8. **Stall detector too short:** after three restarts, Gold's catch-up batches took 30–97 s each, and a "total unchanged for 2 minutes" rule declared failure while Gold was still working. Silver and Gold were correct against the oracles; only the wait was too impatient. It now allows 6 minutes.

## Scaling considerations

Documented, **not demonstrated** on this machine.

- **Throughput ceiling:** Silver processes ~1,000 events/s in a `local[2]` driver, so a 1000x replay builds a backlog (peak lag 94k). The fix is more cores per job or a cluster (Spark on Kubernetes or standalone), not more Kafka.
- **Parallelism:** `trips.raw.v1` has 6 partitions, so up to 6 parallel Kafka read tasks per job. More partitions need a topic change and a decision on the key (`event_id` spreads evenly; a zone key gives locality but hot partitions).
- **Single writer per Delta table:** Delta on S3A has no atomic rename, so each table has exactly one streaming writer. Scale by splitting tables (for example per taxi type) or moving to a catalog/log store that supports concurrent writers.
- **Dedup cost:** the insert-only MERGE on `event_id` scans more of Silver as it grows; partition or bound it by event date before running at much larger volume.
- **Small files:** many small commits per table; `OPTIMIZE`/`VACUUM` compaction is not implemented.
- **Backpressure:** `maxOffsetsPerTrigger` caps batch size on the Spark side (used in the crash drill: batches of ~20,000). The producer side is demonstrated (backpressure events). Spark-side backpressure under overload is only partly evidenced by the lag curves.

## Not tested

- Loss of two brokers (the KRaft quorum), or a rolling restart of all brokers.
- MinIO or Postgres failure and recovery.
- Deleting a checkpoint while keeping the table (expected to re-append, see 3).
- A real multi-node Spark cluster.
