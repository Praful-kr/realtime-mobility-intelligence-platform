COMPOSE = docker compose --env-file .env -f infrastructure/docker-compose.yml
BOOTSTRAP = kafka-1:9092,kafka-2:9092,kafka-3:9092

.PHONY: up down ps logs topics-describe clean

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

topics-describe:
	$(COMPOSE) exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server $(BOOTSTRAP) --describe

# ---- Phase 2 vertical slice ----
SPARK_JOB = $(COMPOSE) --profile jobs run --rm spark-job
PY = PYTHONPATH=. .venv/bin/python

.PHONY: topics produce bronze verify-bronze test
topics:
	$(PY) -m infrastructure.scripts.create_topics

produce:   ## make produce SPEED=10 LIMIT=100000   (SPEED=0 unthrottled)
	$(PY) -m producers.replay_producer --speed $(or $(SPEED),1) --limit $(or $(LIMIT),0)

bronze:    ## drain topic into Bronze then stop; use TRIGGER=5 for continuous
	$(SPARK_JOB) bronze_stream --trigger $(or $(TRIGGER),availableNow)

verify-bronze:
	$(SPARK_JOB) verify_bronze 2>&1 | grep -E "VERIFY_"

test:
	$(PY) -m pytest tests/unit -q

# ---- Phase 3 Silver ----
.PHONY: silver verify-silver dq-report test-spark reset-data e2e-silver
silver:
	$(SPARK_JOB) silver_stream --trigger $(or $(TRIGGER),availableNow)

verify-silver:
	$(SPARK_JOB) verify_silver 2>&1 | grep -E "SILVER_REPORT"

dq-report:
	@$(SPARK_JOB) dq_report 2>/dev/null | sed -n '/^# /,$$p'

test-spark:   ## Spark tests run inside the Spark image (no JVM on the host)
	$(COMPOSE) --profile jobs run --rm --entrypoint bash spark-job /opt/app/infrastructure/spark/run_pytest.sh tests/spark 2>&1 | tail -40

reset-data:   ## DESTRUCTIVE: deletes Kafka topics, lakehouse and checkpoints (local dev data only)
	bash infrastructure/scripts/reset_data.sh

e2e-silver:
	bash infrastructure/scripts/e2e_silver.sh

# ---- Phase 4 Gold ----
.PHONY: gold verify-gold pg-apply psql
gold:      ## TRIGGER=availableNow|<seconds>; WATERMARK="30 minutes"
	$(SPARK_JOB) gold_stream --trigger $(or $(TRIGGER),availableNow) --watermark "$(or $(WATERMARK),30 minutes)"

verify-gold:   ## add LIVE=1 for live/late-data mode
	$(SPARK_JOB) gold_verify $(if $(LIVE),--live) 2>&1 | grep -E "GOLD_RESULT|GOLD_REPORT"

pg-apply:  ## re-apply the (idempotent) serving DDL to a running Postgres
	docker exec -i mobility-postgres-1 psql -U mobility -d mobility -v ON_ERROR_STOP=1 < infrastructure/postgres/init.sql

psql:
	docker exec -it mobility-postgres-1 psql -U mobility -d mobility

# ---- Phase 5 API ----
.PHONY: api-vendor api-up test-api e2e-api
api-vendor:   ## vendor linux/arm64 deps for the API image (no PyPI access needed at docker build)
	rm -rf api/vendor
	uv pip install -q --target api/vendor --python-platform aarch64-unknown-linux-gnu --python-version 3.12 -r api/requirements.txt

api-up: api-vendor
	$(COMPOSE) up -d --build --wait api

test-api:
	$(PY) -m pytest tests/integration/test_api.py -q

e2e-api:
	$(PY) -m tests.e2e.api_check --n $(or $(N),300000) --null-every $(or $(NULLP),1009)

# ---- Phase 6 Observability ----
.PHONY: monitoring-up streaming-up streaming-down dashboards obs-check obs-demo
monitoring-up:
	$(COMPOSE) up -d --wait kafka-exporter prometheus grafana api

streaming-up:   ## long-running Bronze/Silver/Gold jobs (Kafka UI is stopped to save RAM on an 8 GB Docker VM)
	$(COMPOSE) stop kafka-ui
	$(COMPOSE) --profile streaming up -d spark-bronze spark-silver spark-gold

streaming-down:
	$(COMPOSE) --profile streaming stop -t 60 spark-bronze spark-silver spark-gold

dashboards:     ## regenerate dashboards/grafana/*.json from dashboards/build_dashboards.py
	python3 dashboards/build_dashboards.py

obs-check:
	$(PY) -m tests.e2e.observability_check

obs-demo:
	bash infrastructure/scripts/observability_demo.sh
