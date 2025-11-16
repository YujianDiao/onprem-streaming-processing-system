#!/bin/bash
# Main startup script for the data platform

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Data Streaming Platform - Startup${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating .env file from .env.example${NC}"
    cp .env.example .env
fi

# Function to check service health
check_service() {
    local service=$1
    local max_attempts=30
    local attempt=1

    echo -e "${YELLOW}Waiting for $service to be healthy...${NC}"

    while [ $attempt -le $max_attempts ]; do
        if docker compose ps | grep $service | grep -q "healthy\|Up"; then
            echo -e "${GREEN}✓ $service is ready${NC}"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts - waiting for $service..."
        sleep 10
        attempt=$((attempt + 1))
    done

    echo -e "${RED}✗ $service failed to start${NC}"
    return 1
}

# Start infrastructure services
echo -e "\n${GREEN}Step 1: Starting infrastructure services...${NC}"
docker compose up -d kafka-1 kafka-2 kafka-3 postgres minio

check_service "kafka-1"
check_service "postgres"
check_service "minio"

# Initialize MinIO buckets
echo -e "\n${GREEN}Step 2: Initializing MinIO buckets...${NC}"
docker compose up minio-init

# Start Kafka ecosystem
echo -e "\n${GREEN}Step 3: Starting Kafka ecosystem...${NC}"
docker compose up -d schema-registry kafka-ui

check_service "schema-registry"

# Create Kafka topics
echo -e "\n${GREEN}Step 4: Creating Kafka topics...${NC}"
bash scripts/setup-kafka-topics.sh

# Start Hive Metastore
echo -e "\n${GREEN}Step 5: Starting Hive Metastore...${NC}"
docker compose up -d hive-metastore

check_service "hive-metastore"

# Start processing layer
echo -e "\n${GREEN}Step 6: Starting Spark cluster...${NC}"
docker compose up -d spark-master spark-worker-1 spark-worker-2

# Start analytics
echo -e "\n${GREEN}Step 7: Starting Trino...${NC}"
docker compose up -d trino

check_service "trino"

# Start monitoring
echo -e "\n${GREEN}Step 8: Starting monitoring stack...${NC}"
docker compose up -d prometheus grafana

# Start data generator
echo -e "\n${GREEN}Step 9: Starting data generator...${NC}"
docker compose up -d data-generator

# Optional: Start DataHub
read -p "Do you want to start DataHub? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${GREEN}Starting DataHub...${NC}"
    docker compose --profile datahub up -d
fi

# Display service URLs
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Data Lakehouse Platform Started!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\n${YELLOW}Service URLs:${NC}"
echo -e "  Kafka UI:         http://localhost:8080"
echo -e "  Schema Registry:  http://localhost:8081"
echo -e "  Trino:            http://localhost:8086"
echo -e "  Spark Master UI:  http://localhost:8888"
echo -e "  Spark Worker 1:   http://localhost:8081"
echo -e "  Spark Worker 2:   http://localhost:8082"
echo -e "  MinIO Console:    http://localhost:9001 (minioadmin/minioadmin)"
echo -e "  Grafana:          http://localhost:3000 (admin/admin)"
echo -e "  Prometheus:       http://localhost:9090"
echo -e "  PostgreSQL:       localhost:5432 (hive/hive123)"
echo -e "  Hive Metastore:   thrift://localhost:9083"

if docker compose ps | grep -q datahub; then
    echo -e "  DataHub:          http://localhost:9002"
fi

echo -e "\n${YELLOW}Next Steps:${NC}"
echo -e "1. Check Kafka UI to verify streaming data from generator"
echo -e "2. Query data lakehouse using Trino SQL interface"
echo -e "3. View Spark UI to monitor streaming job execution"
echo -e "4. Monitor system health in Grafana dashboards"
echo -e "5. Explore Iceberg tables in MinIO via Trino"

echo -e "\n${GREEN}For logs, use: docker compose logs -f [service-name]${NC}"
echo -e "${GREEN}To stop all services: docker compose down${NC}"
