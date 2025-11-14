.PHONY: help start stop restart status logs clean setup topics datagen test backup

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(GREEN)Data Streaming Platform - Makefile Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(BLUE)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""

setup: ## Initial setup - copy .env and make scripts executable
	@echo "$(YELLOW)Setting up environment...$(NC)"
	@if [ ! -f .env ]; then cp .env.example .env; echo "$(GREEN)✓ Created .env file$(NC)"; fi
	@chmod +x scripts/*.sh
	@echo "$(GREEN)✓ Made scripts executable$(NC)"
	@echo "$(GREEN)Setup complete!$(NC)"

start: ## Start all services
	@echo "$(YELLOW)Starting all services...$(NC)"
	@bash scripts/startup.sh

start-core: ## Start only core services (Kafka, PostgreSQL, MinIO)
	@echo "$(YELLOW)Starting core services...$(NC)"
	@docker-compose up -d kafka-1 kafka-2 kafka-3 postgres minio hive-metastore
	@echo "$(GREEN)✓ Core services started$(NC)"

start-processing: ## Start processing layer (Spark)
	@echo "$(YELLOW)Starting processing layer...$(NC)"
	@docker-compose up -d spark-master spark-worker-1 spark-worker-2
	@echo "$(GREEN)✓ Processing layer started$(NC)"

start-monitoring: ## Start monitoring stack (Prometheus, Grafana)
	@echo "$(YELLOW)Starting monitoring stack...$(NC)"
	@docker-compose up -d prometheus grafana
	@echo "$(GREEN)✓ Monitoring stack started$(NC)"

start-analytics: ## Start analytics tools (Trino)
	@echo "$(YELLOW)Starting analytics tools...$(NC)"
	@docker-compose up -d trino
	@echo "$(GREEN)✓ Analytics tools started$(NC)"

start-datahub: ## Start DataHub (optional)
	@echo "$(YELLOW)Starting DataHub...$(NC)"
	@docker-compose --profile datahub up -d
	@echo "$(GREEN)✓ DataHub started$(NC)"

stop: ## Stop all services
	@echo "$(YELLOW)Stopping all services...$(NC)"
	@docker-compose down
	@echo "$(GREEN)✓ All services stopped$(NC)"

restart: ## Restart all services
	@echo "$(YELLOW)Restarting all services...$(NC)"
	@docker-compose restart
	@echo "$(GREEN)✓ All services restarted$(NC)"

status: ## Show status of all services
	@echo "$(YELLOW)Service Status:$(NC)"
	@docker-compose ps

logs: ## Show logs for all services
	@docker-compose logs -f

logs-kafka: ## Show Kafka logs
	@docker-compose logs -f kafka-1 kafka-2 kafka-3

logs-spark: ## Show Spark logs
	@docker-compose logs -f spark-master spark-worker-1 spark-worker-2

logs-trino: ## Show Trino logs
	@docker-compose logs -f trino

logs-postgres: ## Show PostgreSQL logs
	@docker-compose logs -f postgres

logs-datagen: ## Show Data Generator logs
	@docker-compose logs -f data-generator

topics: ## Create Kafka topics
	@echo "$(YELLOW)Creating Kafka topics...$(NC)"
	@bash scripts/setup-kafka-topics.sh

datagen-start: ## Start data generator
	@echo "$(YELLOW)Starting data generator...$(NC)"
	@docker-compose up -d data-generator
	@echo "$(GREEN)✓ Data generator started$(NC)"

datagen-stop: ## Stop data generator
	@echo "$(YELLOW)Stopping data generator...$(NC)"
	@docker-compose stop data-generator
	@echo "$(GREEN)✓ Data generator stopped$(NC)"

kafka-ui: ## Open Kafka UI in browser
	@echo "$(YELLOW)Opening Kafka UI...$(NC)"
	@open http://localhost:8080 || xdg-open http://localhost:8080

trino-ui: ## Open Trino UI in browser
	@echo "$(YELLOW)Opening Trino UI...$(NC)"
	@open http://localhost:8086 || xdg-open http://localhost:8086

spark-ui: ## Open Spark UI in browser
	@echo "$(YELLOW)Opening Spark UI...$(NC)"
	@open http://localhost:8888 || xdg-open http://localhost:8888

grafana-ui: ## Open Grafana UI in browser
	@echo "$(YELLOW)Opening Grafana UI...$(NC)"
	@open http://localhost:3000 || xdg-open http://localhost:3000

minio-ui: ## Open MinIO Console in browser
	@echo "$(YELLOW)Opening MinIO Console...$(NC)"
	@open http://localhost:9001 || xdg-open http://localhost:9001

all-ui: ## Open all UIs in browser
	@make kafka-ui
	@sleep 1
	@make trino-ui
	@sleep 1
	@make spark-ui
	@sleep 1
	@make grafana-ui
	@sleep 1
	@make minio-ui

shell-kafka: ## Open shell in Kafka container
	@docker exec -it kafka-1 /bin/bash

shell-spark: ## Open shell in Spark master container
	@docker exec -it spark-master /bin/bash

shell-postgres: ## Open PostgreSQL shell
	@docker exec -it postgres psql -U hive -d metastore

shell-trino: ## Open Trino CLI
	@docker exec -it trino trino

shell-minio: ## Open MinIO client shell
	@docker exec -it minio /bin/sh

test-kafka: ## Test Kafka connectivity
	@echo "$(YELLOW)Testing Kafka...$(NC)"
	@docker exec kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092
	@echo "$(GREEN)✓ Kafka is accessible$(NC)"

test-postgres: ## Test PostgreSQL connectivity
	@echo "$(YELLOW)Testing PostgreSQL...$(NC)"
	@docker exec postgres pg_isready -U hive
	@echo "$(GREEN)✓ PostgreSQL is accessible$(NC)"

test-trino: ## Test Trino connectivity
	@echo "$(YELLOW)Testing Trino...$(NC)"
	@curl -f http://localhost:8086/v1/info
	@echo "$(GREEN)✓ Trino is accessible$(NC)"

test-minio: ## Test MinIO connectivity
	@echo "$(YELLOW)Testing MinIO...$(NC)"
	@curl -f http://localhost:9000/minio/health/live
	@echo "$(GREEN)✓ MinIO is accessible$(NC)"

test-all: test-kafka test-postgres test-minio test-trino ## Test all core services
	@echo "$(GREEN)✓ All tests passed!$(NC)"

backup-postgres: ## Backup PostgreSQL database
	@echo "$(YELLOW)Backing up PostgreSQL...$(NC)"
	@mkdir -p backup
	@docker exec postgres pg_dump -U hive metastore > backup/postgres_backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "$(GREEN)✓ PostgreSQL backup complete$(NC)"

backup-minio: ## Backup MinIO data
	@echo "$(YELLOW)Backing up MinIO...$(NC)"
	@mkdir -p backup/minio
	@docker exec minio-client mc mirror --preserve myminio/lakehouse backup/minio/lakehouse
	@echo "$(GREEN)✓ MinIO backup complete$(NC)"

clean: ## Stop services and remove volumes (WARNING: deletes all data!)
	@echo "$(RED)WARNING: This will delete all data!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		echo "$(GREEN)✓ Cleanup complete$(NC)"; \
	else \
		echo "$(YELLOW)Cleanup cancelled$(NC)"; \
	fi

clean-all: ## Complete cleanup including images
	@echo "$(RED)WARNING: This will delete all data and images!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v --rmi all; \
		docker system prune -af; \
		echo "$(GREEN)✓ Complete cleanup done$(NC)"; \
	else \
		echo "$(YELLOW)Cleanup cancelled$(NC)"; \
	fi

rebuild: ## Rebuild custom images (Data Generator)
	@echo "$(YELLOW)Rebuilding custom images...$(NC)"
	@docker-compose build --no-cache data-generator
	@echo "$(GREEN)✓ Rebuild complete$(NC)"

update: ## Pull latest images
	@echo "$(YELLOW)Pulling latest images...$(NC)"
	@docker-compose pull
	@echo "$(GREEN)✓ Images updated$(NC)"

health: ## Check health of all services
	@echo "$(YELLOW)Checking service health...$(NC)"
	@docker-compose ps | grep -E "(healthy|Up)" || echo "$(RED)Some services are not healthy$(NC)"

stats: ## Show resource usage statistics
	@docker stats --no-stream

network: ## Show network information
	@docker network inspect data-platform_data-platform || echo "$(RED)Network not found$(NC)"

volumes: ## List all volumes
	@docker volume ls | grep data-platform

urls: ## Show all service URLs
	@echo "$(GREEN)Service URLs:$(NC)"
	@echo "  Kafka UI:          http://localhost:8080"
	@echo "  Schema Registry:   http://localhost:8081"
	@echo "  Trino:             http://localhost:8086"
	@echo "  Spark Master:      http://localhost:8888"
	@echo "  MinIO Console:     http://localhost:9001"
	@echo "  Grafana:           http://localhost:3000"
	@echo "  Prometheus:        http://localhost:9090"
	@echo "  Hive Metastore:    thrift://localhost:9083"
	@echo "  PostgreSQL:        localhost:5432"

version: ## Show versions of all components
	@echo "$(YELLOW)Component Versions:$(NC)"
	@grep "image:" docker-compose.yml | grep -v "#" | awk '{print $$2}'
