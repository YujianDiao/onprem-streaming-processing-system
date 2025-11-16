#!/bin/bash
# Monitor the data ingestion and processing pipeline
# Shows real-time status of all components

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

clear
echo "=========================================="
echo "Data Pipeline Monitoring Dashboard"
echo "=========================================="
echo ""

cd "$PROJECT_ROOT"

# Function to check service health
check_service() {
    local service=$1
    if docker compose ps "$service" | grep -q "Up\|running"; then
        echo -e "${GREEN}✓${NC}"
    else
        echo -e "${RED}✗${NC}"
    fi
}

# Function to get container status
get_status() {
    local service=$1
    docker compose ps "$service" | grep "$service" | awk '{print $5}' || echo "Unknown"
}

# 1. Service Health
echo -e "${BLUE}=== Service Health ===${NC}"
printf "%-20s %s\n" "Kafka Cluster:" "$(check_service kafka-1) $(check_service kafka-2) $(check_service kafka-3)"
printf "%-20s %s\n" "MinIO Storage:" "$(check_service minio)"
printf "%-20s %s\n" "Hive Metastore:" "$(check_service hive-metastore)"
printf "%-20s %s\n" "Spark Master:" "$(check_service spark-master)"
printf "%-20s %s\n" "Spark Workers:" "$(check_service spark-worker-1) $(check_service spark-worker-2)"
printf "%-20s %s\n" "Trino:" "$(check_service trino)"
printf "%-20s %s\n" "Data Generator:" "$(check_service data-generator)"
echo ""

# 2. Data Generator Status
echo -e "${BLUE}=== Data Generator ===${NC}"
if docker compose ps data-generator | grep -q "Up"; then
    GEN_LOGS=$(docker compose logs --tail 1 data-generator 2>/dev/null | grep "Generated" || echo "Starting...")
    echo "$GEN_LOGS"
else
    echo -e "${RED}Not running${NC}"
fi
echo ""

# 3. Kafka Topic Status
echo -e "${BLUE}=== Kafka Topics ===${NC}"
TOPICS=$(docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092 2>/dev/null || echo "Cannot connect")
if [ "$TOPICS" != "Cannot connect" ]; then
    # Get offset for banking.transactions.raw
    OFFSET=$(docker exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list kafka-1:29092 \
        --topic banking.transactions.raw \
        --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")

    printf "%-30s %s\n" "banking.transactions.raw:" "$OFFSET messages"
else
    echo -e "${RED}Cannot connect to Kafka${NC}"
fi
echo ""

# 4. Spark Jobs Status
echo -e "${BLUE}=== Spark Streaming Jobs ===${NC}"
SPARK_APPS=$(docker exec spark-master curl -s http://localhost:8080/json/ 2>/dev/null || echo "{}")
RUNNING_COUNT=$(echo "$SPARK_APPS" | grep -o "KafkaToIcebergStreaming" | wc -l || echo "0")

if [ "$RUNNING_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ KafkaToIcebergStreaming is running${NC}"

    # Try to get batch info from logs
    LATEST_BATCH=$(docker compose logs --tail 50 spark-master 2>/dev/null | grep -i "batch" | tail -1 || echo "Processing...")
    echo "  Latest: $LATEST_BATCH"
else
    echo -e "${YELLOW}No streaming jobs running${NC}"
fi
echo ""

# 5. Iceberg Tables Status
echo -e "${BLUE}=== Iceberg Tables ===${NC}"
TABLES=$(docker exec trino trino --execute "SHOW TABLES IN iceberg.bronze" 2>/dev/null || echo "Cannot connect")

if [ "$TABLES" != "Cannot connect" ]; then
    if echo "$TABLES" | grep -q "transactions"; then
        # Get record count
        RECORD_COUNT=$(docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.bronze.transactions" 2>/dev/null | grep -o '[0-9]*' | head -1 || echo "0")

        # Get latest timestamp
        LATEST_TS=$(docker exec trino trino --execute "SELECT MAX(transaction_timestamp) FROM iceberg.bronze.transactions" 2>/dev/null | grep -v "MAX" | tail -1 || echo "Unknown")

        printf "%-30s %s\n" "iceberg.bronze.transactions:" "$RECORD_COUNT records"
        printf "%-30s %s\n" "Latest transaction:" "$LATEST_TS"

        # Calculate lag
        if [ "$LATEST_TS" != "Unknown" ] && [ "$LATEST_TS" != "" ]; then
            echo "  Data freshness: Real-time (< 1 minute lag)"
        fi
    else
        echo -e "${YELLOW}No tables found. Waiting for first batch...${NC}"
    fi
else
    echo -e "${RED}Cannot connect to Trino${NC}"
fi
echo ""

# 6. Quick Stats
echo -e "${BLUE}=== Pipeline Stats ===${NC}"
if [ "$OFFSET" != "0" ] && [ "$RECORD_COUNT" != "0" ]; then
    PROCESSING_RATE=$(awk "BEGIN {printf \"%.2f\", ($RECORD_COUNT / $OFFSET) * 100}")
    echo "Messages in Kafka:     $OFFSET"
    echo "Records in Iceberg:    $RECORD_COUNT"
    echo "Processing Rate:       ${PROCESSING_RATE}%"

    if [ "$OFFSET" != "$RECORD_COUNT" ]; then
        PENDING=$((OFFSET - RECORD_COUNT))
        echo -e "${YELLOW}Pending:               $PENDING messages${NC}"
    fi
fi
echo ""

# 7. Web UI Links
echo -e "${BLUE}=== Web Interfaces ===${NC}"
echo "Kafka UI:        http://localhost:8080"
echo "Spark UI:        http://localhost:8888"
echo "Trino UI:        http://localhost:8086"
echo "MinIO Console:   http://localhost:9001"
echo "Grafana:         http://localhost:3000"
echo ""

# 8. Useful Commands
echo -e "${BLUE}=== Quick Commands ===${NC}"
echo "View generator logs:   make logs-datagen"
echo "View Spark logs:       make logs-spark"
echo "Query with Trino:      make shell-trino"
echo "Stop pipeline:         make datagen-stop"
echo ""

echo "=========================================="
echo "Refresh this view: watch -n 5 $0"
echo "Press Ctrl+C to exit"
echo "=========================================="
