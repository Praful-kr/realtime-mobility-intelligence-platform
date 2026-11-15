# Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Containers exit with code 137, or Spark jobs hang | The Docker VM is out of memory. Use an 8 GB VM and keep Kafka UI stopped while the three Spark drivers run (`make streaming-up` does this) |
| `make verify-*` or a drill times out | Do not run a fourth Spark JVM next to three live drivers. Stop the streaming services first (the drills do this) |
| `docker kill` does not restart a Spark service | Docker never applies a restart policy to a manual kill. A real crash does restart it (`restart: on-failure`) |
| Silver or Gold wait forever at start | They wait for the upstream Delta table (`WAITING_FOR_UPSTREAM` in the log). Start Bronze and produce data |
| API returns 503 | Postgres is unreachable or the connection pool timed out. `/health/live` stays 200 and `/health/ready` returns 503 |
| `Postgres not reachable on localhost:5433` in tests | Start the stack with `make up`, or let CI's service container provide it |
| Docker build cannot reach PyPI | The API image uses vendored dependencies. Run `make api-vendor` first |
| Reset everything | `bash infrastructure/scripts/reset_data.sh`. This **deletes** topics, the lakehouse, checkpoints and the Gold tables |
