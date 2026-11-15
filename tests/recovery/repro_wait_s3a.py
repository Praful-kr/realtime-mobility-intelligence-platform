"""Cross-process repro on the REAL object store:  repro_wait_s3a.py wait <path> <seconds>  /  write <path>.
A waiter polls with wait_for_delta_table while ANOTHER process (another container) creates the table."""
import sys
import time

from spark_jobs.common import build_spark, wait_for_delta_table

mode, path = sys.argv[1], sys.argv[2]
spark = build_spark(f"repro-{mode}")
if mode == "wait":
    t0 = time.time()
    try:
        wait_for_delta_table(spark, path, poll_seconds=2, timeout_seconds=int(sys.argv[3]))
        print(f"RESULT FOUND after {time.time() - t0:.1f}s", flush=True)
    except TimeoutError:
        print(f"RESULT STUCK after {time.time() - t0:.1f}s: table exists but the waiter never saw it", flush=True)
else:
    spark.range(5).write.format("delta").save(path)
    print("TABLE WRITTEN", flush=True)
