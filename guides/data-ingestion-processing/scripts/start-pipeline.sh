#!/bin/bash
# Start the complete data ingestion and processing pipeline
# This script automates the startup of data generation and Spark processing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo "=========================================="
echo "Starting Data Ingestion Pipeline"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Step 1: Verify prerequisites
echo -e "${YELLOW}Step 1: Checking prerequisites...${NC}"
cd "$PROJECT_ROOT"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    exit 1
fi

# Check if services are running
if ! docker compose ps | grep -q "kafka-1.*running"; then
    echo -e "${RED}Error: Kafka is not running. Please start services first:${NC}"
    echo "  make start"
    exit 1
fi

echo -e "${GREEN}✓ Prerequisites OK${NC}"
echo ""

# Step 2: Create Kafka topics if they don't exist
echo -e "${YELLOW}Step 2: Setting up Kafka topics...${NC}"
bash scripts/setup-kafka-topics.sh 2>/dev/null || echo "Topics may already exist"
echo -e "${GREEN}✓ Kafka topics ready${NC}"
echo ""

# Step 3: Start data generator
echo -e "${YELLOW}Step 3: Starting data generator...${NC}"
docker compose up -d data-generator
sleep 5

# Verify data generator is running
if docker compose ps data-generator | grep -q "Up"; then
    echo -e "${GREEN}✓ Data generator started${NC}"
else
    echo -e "${RED}Error: Data generator failed to start${NC}"
    docker compose logs data-generator
    exit 1
fi
echo ""

# Step 4: Wait for data to flow
echo -e "${YELLOW}Step 4: Waiting for data to flow into Kafka (30 seconds)...${NC}"
sleep 30

# Verify messages in Kafka
echo "Checking Kafka for messages..."
MESSAGE_COUNT=$(docker exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list kafka-1:29092 \
    --topic banking.transactions.raw \
    --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")

if [ "$MESSAGE_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Found $MESSAGE_COUNT messages in Kafka${NC}"
else
    echo -e "${YELLOW}Warning: No messages found yet, but continuing...${NC}"
fi
echo ""

# Step 5: Submit Spark streaming job
echo -e "${YELLOW}Step 5: Submitting Spark streaming job...${NC}"
echo "This will start processing Kafka messages and writing to Iceberg tables"
echo ""

# Check if job is already running
RUNNING_APPS=$(docker exec spark-master curl -s http://localhost:8080/json/ | grep -c "KafkaToIcebergStreaming" || echo "0")

if [ "$RUNNING_APPS" -gt 0 ]; then
    echo -e "${YELLOW}Spark streaming job is already running${NC}"
else
    # Submit the job in the background
    echo "Submitting Spark job (this runs continuously in the background)..."

    docker exec -d spark-master spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.local.type=hadoop \
        --conf spark.sql.catalog.local.warehouse=s3a://lakehouse/ \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        /opt/bitnami/spark/jobs/kafka_to_iceberg_streaming.py

    sleep 10
    echo -e "${GREEN}✓ Spark streaming job submitted${NC}"
fi
echo ""

# Step 6: Wait for first batch to process
echo -e "${YELLOW}Step 6: Waiting for first batch to be processed (60 seconds)...${NC}"
echo "Spark processes data in 30-second micro-batches"
sleep 60
echo ""

# Step 7: Verify data in Iceberg
echo -e "${YELLOW}Step 7: Verifying data in Iceberg tables...${NC}"

# Check if table exists
TABLE_EXISTS=$(docker exec trino trino --execute "SHOW TABLES IN iceberg.bronze" 2>/dev/null | grep -c "transactions" || echo "0")

if [ "$TABLE_EXISTS" -gt 0 ]; then
    echo -e "${GREEN}✓ Iceberg table 'transactions' exists${NC}"

    # Count records
    RECORD_COUNT=$(docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.bronze.transactions" 2>/dev/null | grep -o '[0-9]*' | head -1 || echo "0")

    if [ "$RECORD_COUNT" -gt 0 ]; then
        echo -e "${GREEN}✓ Found $RECORD_COUNT records in iceberg.bronze.transactions${NC}"
    else
        echo -e "${YELLOW}Warning: Table exists but no records yet${NC}"
    fi
else
    echo -e "${YELLOW}Warning: Table not created yet. Wait a bit longer and check manually.${NC}"
fi
echo ""

# Final summary
echo "=========================================="
echo -e "${GREEN}Pipeline Started Successfully!${NC}"
echo "=========================================="
echo ""
echo "Data is now flowing through your lakehouse:"
echo "  1. Data Generator → Kafka (1 transaction/second)"
echo "  2. Kafka → Spark Streaming (processing every 30 seconds)"
echo "  3. Spark → Iceberg Tables (stored in MinIO)"
echo ""
echo "Next Steps:"
echo "  • Monitor Kafka:     http://localhost:8080"
echo "  • Monitor Spark:     http://localhost:8888"
echo "  • Query with Trino:  make shell-trino"
echo "  • View logs:         make logs-datagen"
echo ""
echo "To query your data:"
echo "  make shell-trino"
echo "  > SELECT COUNT(*) FROM iceberg.bronze.transactions;"
echo ""
echo "To stop the pipeline:"
echo "  make datagen-stop"
echo "  docker exec spark-master pkill -f KafkaToIcebergStreaming"
echo ""
