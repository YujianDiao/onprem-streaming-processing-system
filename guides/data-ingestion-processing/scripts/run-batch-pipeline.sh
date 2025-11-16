#!/bin/bash
# Run Spark Batch Aggregations Pipeline
# Processes Bronze → Silver → Gold layers

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "=========================================="
echo "Spark Batch Aggregations Pipeline"
echo "=========================================="
echo ""

cd "$PROJECT_ROOT"

# Check if Bronze table has data
echo -e "${YELLOW}Checking Bronze layer data...${NC}"
BRONZE_COUNT=$(docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.bronze.transactions" 2>/dev/null | grep -o '[0-9]*' | head -1 || echo "0")

if [ "$BRONZE_COUNT" -eq 0 ]; then
    echo -e "${RED}Error: No data in Bronze layer${NC}"
    echo "Please run the streaming pipeline first:"
    echo "  bash guides/data-ingestion-processing/scripts/start-pipeline.sh"
    exit 1
fi

echo -e "${GREEN}✓ Found $BRONZE_COUNT records in Bronze layer${NC}"
echo ""

# Submit batch job
echo -e "${YELLOW}Submitting Spark batch aggregations job...${NC}"
echo "This will process:"
echo "  1. Bronze → Silver (data cleaning & validation)"
echo "  2. Silver → Gold (business aggregations)"
echo "  3. Additional merchant analytics"
echo ""

docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
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
  /opt/bitnami/spark/jobs/batch_aggregations.py

echo ""
echo "=========================================="
echo -e "${GREEN}Batch Processing Complete!${NC}"
echo "=========================================="
echo ""

# Verify results
echo -e "${BLUE}Checking results...${NC}"

# Check Silver layer
SILVER_COUNT=$(docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.silver.transactions" 2>/dev/null | grep -o '[0-9]*' | head -1 || echo "0")
if [ "$SILVER_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Silver layer: $SILVER_COUNT records${NC}"
else
    echo -e "${YELLOW}⚠ Silver layer: No records found${NC}"
fi

# Check Gold layer
GOLD_COUNT=$(docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.gold.daily_account_summary" 2>/dev/null | grep -o '[0-9]*' | head -1 || echo "0")
if [ "$GOLD_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Gold layer (daily_account_summary): $GOLD_COUNT records${NC}"
else
    echo -e "${YELLOW}⚠ Gold layer: No records found${NC}"
fi

# Check Merchant analytics
MERCHANT_COUNT=$(docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.gold.merchant_summary" 2>/dev/null | grep -o '[0-9]*' | head -1 || echo "0")
if [ "$MERCHANT_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Merchant summary: $MERCHANT_COUNT records${NC}"
else
    echo -e "${YELLOW}⚠ Merchant summary: No records found${NC}"
fi

echo ""
echo "Query your processed data:"
echo "  make shell-trino"
echo ""
echo "Example queries:"
echo "  -- View cleaned transactions"
echo "  SELECT * FROM iceberg.silver.transactions LIMIT 10;"
echo ""
echo "  -- View daily account summaries"
echo "  SELECT * FROM iceberg.gold.daily_account_summary LIMIT 10;"
echo ""
echo "  -- View merchant analytics"
echo "  SELECT * FROM iceberg.gold.merchant_summary"
echo "  ORDER BY total_amount DESC LIMIT 10;"
echo ""
