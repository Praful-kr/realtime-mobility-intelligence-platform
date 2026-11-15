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
