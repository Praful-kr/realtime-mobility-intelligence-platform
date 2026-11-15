"""Fault-injection hook for the reliability drills. OFF unless CHAOS_CRASH_AFTER_WRITE is set, e.g. "bronze:1".

It crashes the driver exactly ONCE (a marker object records that it already happened, so the restarted job does not
crash again) right AFTER a micro-batch was written to the sink but BEFORE Spark commits the source offsets: the one
window in which at-least-once replay could create duplicates. Used by tests/recovery/drill_crash_after_write.sh.
"""
from __future__ import annotations

import os
import sys


def maybe_crash(spark, job: str, batch_id: int) -> None:
    spec = os.getenv("CHAOS_CRASH_AFTER_WRITE", "")
    if spec != f"{job}:{batch_id}":
        return
    jvm = spark._jvm
    marker = jvm.org.apache.hadoop.fs.Path(f"{os.getenv('CHECKPOINT_ROOT', 's3a://checkpoints')}/_chaos/{job}-{batch_id}.done")
    fs = marker.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(marker):
        print(f"CHAOS_SKIP already crashed once for {spec}", flush=True)
        return
    fs.create(marker).close()
    print(f"CHAOS_CRASH after sink write of {job} batch {batch_id}, before the offset commit", flush=True)
    sys.stdout.flush()
    os._exit(137)   # abrupt: no finally blocks, no clean shutdown, like a kill -9
