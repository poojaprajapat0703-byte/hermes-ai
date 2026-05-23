# =============================================================================
# Hermes — Developer Makefile
# Usage: make <target>
# Run `make help` to see all available commands
# =============================================================================

.PHONY: help up down restart logs ps \
        kafka-topics kafka-test \
        db-shell redis-shell \
        lint typecheck format \
        clean nuke

# Default target
.DEFAULT_GOAL := help

# Colours for help output
BOLD  := \033[1m
RESET := \033[0m
CYAN  := \033[36m

help: ## Show this help
	@echo ""
	@echo "$(BOLD)Hermes — available make targets$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

up: ## Start all services (detached)
	docker compose up -d
	@echo "✓ Services starting. Run 'make ps' to check health."

down: ## Stop all services (preserve volumes)
	docker compose down

restart: ## Restart all services
	docker compose restart

logs: ## Tail logs from all services
	docker compose logs -f

logs-kafka: ## Tail Kafka logs only
	docker compose logs -f kafka

ps: ## Show service health status
	docker compose ps

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

kafka-topics: ## Create all Hermes Kafka topics
	@chmod +x scripts/create_kafka_topics.sh
	@bash scripts/create_kafka_topics.sh

kafka-test: ## Run producer/consumer smoke test
	uv run python scripts/kafka_producer_test.py

kafka-ui: ## Open Kafka UI in browser
	@open http://localhost:8080 || xdg-open http://localhost:8080

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

db-shell: ## Open psql shell in Postgres container
	docker exec -it hermes-postgres psql -U hermes -d hermes_db

redis-shell: ## Open redis-cli in Redis container
	docker exec -it hermes-redis redis-cli

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

lint: ## Run ruff linter
	uv run ruff check .

format: ## Run ruff formatter
	uv run ruff format .

typecheck: ## Run mypy type checker
	uv run mypy app/

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

clean: ## Stop services and remove containers
	docker compose down --remove-orphans

nuke: ## DANGER: Stop and remove containers + volumes (full reset)
	docker compose down -v --remove-orphans
	@echo "⚠ All volumes wiped. Fresh start on next 'make up'."