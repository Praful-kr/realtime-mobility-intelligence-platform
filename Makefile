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
