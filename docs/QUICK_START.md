# Quick Start Guide

Get your on-premise lakehouse system up and running in minutes!

## Prerequisites

**System Requirements**:
- Docker 20.10+
- Docker Compose 2.0+
- Minimum: 8 CPUs, 16GB RAM
- Recommended: 16 CPUs, 32GB RAM

**Check your system**:
```bash
# Verify Docker
docker --version

# Verify Docker Compose (note: 'docker compose' not 'docker-compose')
docker compose version

# Check available resources
docker info | grep -E "CPUs|Total Memory"
```

---

## Installation

### 1. Start All Services

```bash
# Navigate to project directory
cd onprem-streaming-processing-system

# Start entire platform
docker compose up -d

# Wait 2-3 minutes for all services to initialize
```

### 2. Verify Services Are Running

```bash
# Check service status
docker compose ps

# Expected output: All services should show "Up" or "healthy"
# Key services:
# - minio (ports 9000, 9001)
# - postgres (port 5432)
# - hive-metastore (port 9083)
# - kafka-broker-1,2,3 (ports 9092-9094)
# - spark-master (ports 7077, 8081)
# - spark-worker (port 8082)
# - trino (port 8080)
# - superset (port 8088)
```

---

## Access Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| **Superset** (Visualization) | http://localhost:8088 | admin/admin |
| **Trino Web UI** | http://localhost:8080 | None |
| **Spark Master UI** | http://localhost:8081 | None |
| **Spark Worker UI** | http://localhost:8082 | None |
| **MinIO Console** | http://localhost:9001 | minioadmin/minioadmin |

---

## Complete End-to-End Workflow

### Step 1: Generate Sample Data

The system includes a Python data generator that creates realistic banking transactions.

```bash
# Generate 10,000 sample transactions
docker exec kafka-broker-1 python /scripts/data_generator.py \
  --num-records 10000 \
  --topic banking.transactions.raw

# Expected output:
# Sent 10000 records to topic banking.transactions.raw
```

**What happens**: JSON transaction records are sent to Kafka topic `banking.transactions.raw`.

**Sample transaction**:
```json
{
  "transaction_id": "TXN-12345",
  "account_id": "ACC-67890",
  "timestamp": "2025-11-17T10:30:00Z",
  "transaction_type": "PURCHASE",
  "amount": 125.50,
  "currency": "USD",
  "merchant": "Amazon",
  "status": "COMPLETED"
}
```

### Step 2: Start Streaming Pipeline

Start the Spark Structured Streaming job to process Kafka data into the Bronze layer.

```bash
# Submit streaming job
./submit-streaming-job.sh

# Expected output:
# Streaming job submitted to Spark cluster
```

**What happens**:
- Spark reads from Kafka topic `banking.transactions.raw`
- Data is written to Iceberg table `iceberg.bronze.transactions`
- Files stored in MinIO at `s3a://lakehouse/bronze/transactions/`
- Processes micro-batches every 30 seconds
- Uses checkpoint-based offset management (stored in MinIO)

**Monitor the job**:
```bash
# View Spark Master UI
open http://localhost:8081

# Check streaming job logs
docker logs spark-master | grep -i streaming

# Verify data is being written
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.bronze.transactions;"
```

### Step 3: Verify Bronze Layer Data

Check that data has been ingested into the Bronze layer.

```bash
# Connect to Trino CLI
docker exec -it trino trino --catalog iceberg --schema bronze

# Run verification queries
```

**Verification queries**:
```sql
-- Count total records
SELECT COUNT(*) FROM transactions;
-- Expected: 10,000+ records

-- Check data freshness
SELECT MAX(ingestion_timestamp) as latest_ingestion
FROM transactions;
-- Should be within last few minutes

-- View sample records
SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    merchant,
    timestamp
FROM transactions
LIMIT 10;

-- Distribution by transaction type
SELECT
    transaction_type,
    COUNT(*) as count,
    ROUND(AVG(amount), 2) as avg_amount
FROM transactions
GROUP BY transaction_type
ORDER BY count DESC;
```

**Exit Trino**: Type `quit` or press Ctrl+D

### Step 4: Run Batch Aggregations (Silver & Gold Layers)

Process Bronze data into cleaned Silver layer and aggregated Gold layer.

```bash
# Run batch aggregation job
./submit-batch-job.sh

# Expected output:
# Processing bronze → silver transformations...
# Creating gold layer aggregations...
# Job completed successfully
```

**What happens**:
1. **Bronze → Silver**: Data cleaning, validation, deduplication
2. **Silver → Gold**: Business-level aggregations
   - `daily_account_summary`: Daily account-level metrics
   - `merchant_summary`: Merchant-level transaction summaries

**Verify batch job completed**:
```bash
# Check silver layer
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.silver.transactions;"

# Check gold layers
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.gold.daily_account_summary;"

docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.gold.merchant_summary;"
```

### Step 5: Query the Lakehouse with Trino

Now you have a complete medallion architecture: Bronze → Silver → Gold.

```bash
# Connect to Trino CLI
docker exec -it trino trino --catalog iceberg --schema gold
```

**Sample analytical queries**:

```sql
-- Daily account summary
SELECT
    account_id,
    transaction_date,
    total_transactions,
    total_debits,
    total_credits,
    net_change
FROM daily_account_summary
ORDER BY transaction_date DESC, net_change DESC
LIMIT 20;

-- Top merchants by transaction volume
SELECT
    merchant,
    total_transactions,
    total_amount,
    avg_transaction_amount,
    unique_accounts
FROM merchant_summary
ORDER BY total_amount DESC
LIMIT 10;

-- Cross-layer query (joining silver and gold)
SELECT
    m.merchant,
    m.total_transactions,
    COUNT(DISTINCT s.account_id) as unique_customers,
    SUM(s.amount) as total_revenue
FROM iceberg.gold.merchant_summary m
JOIN iceberg.silver.transactions s ON m.merchant = s.merchant
GROUP BY m.merchant, m.total_transactions
ORDER BY total_revenue DESC
LIMIT 10;
```

**Advanced queries**:

```sql
-- Time travel: Query bronze table as of 1 hour ago
SELECT COUNT(*)
FROM iceberg.bronze.transactions
FOR TIMESTAMP AS OF CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- View table snapshots (Iceberg feature)
SELECT
    committed_at,
    snapshot_id,
    operation,
    summary['added-records'] as records_added
FROM iceberg.bronze.transactions.snapshots
ORDER BY committed_at DESC;

-- Check table files and partitions
SELECT
    file_path,
    record_count,
    file_size_in_bytes / 1024 / 1024 as size_mb
FROM iceberg.bronze.transactions.files
ORDER BY record_count DESC
LIMIT 10;
```

### Step 6: Visualize Data with Superset

Apache Superset provides a modern BI interface to query and visualize your lakehouse data.

#### 6.1 Initial Setup

```bash
# Access Superset
open http://localhost:8088

# Login credentials:
# Username: admin
# Password: admin
```

#### 6.2 Connect Trino Database

1. Click **Settings** → **Database Connections** → **+ Database**
2. Select **Trino** from the dropdown
3. Enter connection details:
   - **Display Name**: `Iceberg Lakehouse`
   - **SQLAlchemy URI**: `trino://trino@trino:8080/iceberg`
   - **Note**: The `trino@` username is required
4. Click **Test Connection** (should show "Connection looks good!")
5. Click **Connect**

#### 6.3 Create Your First Dataset

1. Go to **Data** → **Datasets** → **+ Dataset**
2. Select:
   - **Database**: Iceberg Lakehouse
   - **Schema**: gold
   - **Table**: merchant_summary
3. Click **Create Dataset and Create Chart**

#### 6.4 Create a Chart

**Example: Top Merchants by Revenue**

1. Chart Type: **Bar Chart**
2. Configuration:
   - **Metrics**: `SUM(total_amount)`
   - **Dimensions**: `merchant`
   - **Sort by**: `SUM(total_amount)` descending
   - **Row limit**: 10
3. Click **Update Chart**
4. Click **Save** and give it a name

#### 6.5 Create a Dashboard

1. Go to **Dashboards** → **+ Dashboard**
2. Name it "Banking Analytics Overview"
3. Drag and drop your saved charts
4. Add more charts for:
   - Daily transaction volume (line chart)
   - Transaction type distribution (pie chart)
   - Account activity metrics (table)

#### 6.6 SQL Lab (Advanced Queries)

1. Go to **SQL** → **SQL Lab**
2. Select:
   - **Database**: Iceberg Lakehouse
   - **Schema**: gold (or bronze/silver)
3. Write any SQL query and click **Run**

**Example query**:
```sql
SELECT
    merchant,
    total_transactions,
    total_amount,
    avg_transaction_amount
FROM merchant_summary
WHERE total_amount > 1000
ORDER BY total_amount DESC
LIMIT 20;
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                  Complete Data Flow                          │
└─────────────────────────────────────────────────────────────┘

DATA GENERATION
    Python Script
    │
    ↓ (Kafka Producer)
    │
INGESTION LAYER
    Kafka Topic: banking.transactions.raw
    ├── Broker 1 (port 9092)
    ├── Broker 2 (port 9093)
    └── Broker 3 (port 9094)
    │
    ↓ (Spark Structured Streaming)
    │
BRONZE LAYER (Raw Data)
    iceberg.bronze.transactions
    └── MinIO: s3a://lakehouse/bronze/transactions/
    └── Format: Parquet + GZIP
    └── Status: 22,841+ records (real-time)
    │
    ↓ (Spark Batch Job - Cleaning & Validation)
    │
SILVER LAYER (Cleaned Data)
    iceberg.silver.transactions
    └── MinIO: s3a://lakehouse/silver/transactions/
    └── Deduplication, validation, type conversion
    │
    ↓ (Spark Batch Job - Aggregation)
    │
GOLD LAYER (Business Aggregations)
    ├── iceberg.gold.daily_account_summary
    │   └── Account-level daily metrics
    └── iceberg.gold.merchant_summary
        └── Merchant-level transaction summaries
    │
    ↓ (Trino Query Engine)
    │
QUERY & ANALYTICS
    ├── Trino CLI (SQL interface)
    └── Apache Superset (BI dashboards)

METADATA LAYER
    Hive Metastore (PostgreSQL)
    └── Table schemas, partitions, snapshots

STORAGE LAYER
    MinIO (S3-compatible)
    └── Parquet files + Iceberg metadata
```

---

## Key Concepts

### Medallion Architecture

**Bronze Layer** (Raw):
- Exact copy of source data
- No transformations
- Immutable audit trail
- Query: `iceberg.bronze.transactions`

**Silver Layer** (Cleaned):
- Validated and cleaned
- Deduplicated
- Type conversions applied
- Query: `iceberg.silver.transactions`

**Gold Layer** (Aggregated):
- Business-level aggregations
- Ready for BI tools
- Optimized for analytics
- Query: `iceberg.gold.*`

### Checkpoint-Based Offset Management

**Why you don't see Spark in Kafka consumer groups**:
- Spark Structured Streaming uses **checkpoint-based offset management**
- Offsets stored in MinIO checkpoint directory (not Kafka consumer groups)
- Location: `s3a://lakehouse/checkpoints/kafka-streaming/`
- This is the **recommended** approach for Spark Structured Streaming
- Provides fault tolerance and exactly-once semantics

**Monitoring streaming progress**:
```bash
# View checkpoint metadata
docker exec minio mc ls local/lakehouse/checkpoints/kafka-streaming/offsets/

# Check Spark UI for streaming query status
open http://localhost:8081
```

### Apache Iceberg Features

**ACID Transactions**: Reliable updates, deletes, and concurrent writes

**Time Travel**: Query historical data snapshots
```sql
SELECT * FROM iceberg.bronze.transactions
FOR TIMESTAMP AS OF TIMESTAMP '2025-11-17 10:00:00';
```

**Schema Evolution**: Add/modify columns without rewriting data
```sql
ALTER TABLE iceberg.bronze.transactions
ADD COLUMN new_field VARCHAR;
```

**Hidden Partitioning**: Automatic partition management
```sql
-- Data automatically partitioned by date_partition
-- No need to specify partitions in queries
```

---

## Common Operations

### Generate More Data

```bash
# Generate another batch of transactions
docker exec kafka-broker-1 python /scripts/data_generator.py \
  --num-records 5000 \
  --topic banking.transactions.raw

# Data will be automatically picked up by streaming job
# Check bronze table count after 1-2 minutes
```

### Re-run Batch Aggregations

```bash
# Run daily (or on-demand)
./submit-batch-job.sh

# This will:
# 1. Process new bronze records → silver
# 2. Update gold layer aggregations
```

### Monitor Streaming Job

```bash
# Check if streaming job is running
docker exec spark-master ps aux | grep kafka_to_iceberg

# View streaming logs
docker logs spark-master | tail -100

# View Spark UI
open http://localhost:8081
```

### Query Different Layers

```bash
# Bronze layer (raw data)
docker exec trino trino --catalog iceberg --schema bronze

# Silver layer (cleaned data)
docker exec trino trino --catalog iceberg --schema silver

# Gold layer (aggregations)
docker exec trino trino --catalog iceberg --schema gold
```

### Verify Data Freshness

```bash
# Check latest data in bronze
docker exec trino trino --execute \
  "SELECT MAX(ingestion_timestamp) FROM iceberg.bronze.transactions;"

# Check record count
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.bronze.transactions;"
```

### View MinIO Storage

```bash
# Access MinIO Console
open http://localhost:9001
# Login: minioadmin/minioadmin

# Or use CLI
docker exec minio mc ls local/lakehouse/bronze/transactions/data/
```

---

## Troubleshooting

### No Data in Bronze Table

**Check 1: Verify Kafka has messages**
```bash
docker exec kafka-broker-1 kafka-console-consumer.sh \
  --topic banking.transactions.raw \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

**Check 2: Verify streaming job is running**
```bash
docker exec spark-master ps aux | grep kafka_to_iceberg

# If not running, restart:
./submit-streaming-job.sh
```

**Check 3: Check streaming logs**
```bash
docker logs spark-master | grep -i "exception\|error"
```

### Trino Queries Failing

**Check 1: Verify Hive Metastore is running**
```bash
docker compose ps hive-metastore

# Check logs
docker logs hive-metastore | tail -50
```

**Check 2: Verify Trino can connect**
```bash
docker exec trino trino --execute "SHOW CATALOGS;"
# Should show: iceberg, system

docker exec trino trino --execute "SHOW SCHEMAS IN iceberg;"
# Should show: bronze, silver, gold
```

**Check 3: Verify table exists**
```bash
docker exec trino trino --execute \
  "SHOW TABLES IN iceberg.bronze;"
```

### Superset Can't Connect to Trino

**Issue**: Authentication error or connection refused

**Solution**: Verify connection string includes username
```
Correct:   trino://trino@trino:8080/iceberg
Incorrect: trino://trino:8080/iceberg
```

**Test Trino connectivity**:
```bash
docker exec trino trino --execute "SELECT 1"
```

### Batch Job Fails

**Check 1: Verify bronze data exists**
```bash
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.bronze.transactions;"
```

**Check 2: Check Spark logs**
```bash
docker logs spark-master | grep -i batch_aggregations
```

**Check 3: Verify schemas exist**
```bash
docker exec trino trino --execute "SHOW SCHEMAS IN iceberg;"
```

### Services Won't Start

**Check 1: Verify Docker resources**
```bash
docker info | grep -E "CPUs|Total Memory"
# Need at least 8 CPUs, 16GB RAM
```

**Check 2: Check service logs**
```bash
# View all services
docker compose ps

# Check specific service
docker logs <service-name>
```

**Check 3: Restart services**
```bash
# Restart all
docker compose restart

# Restart specific service
docker compose restart hive-metastore
```

---

## Performance Optimization

### Streaming Throughput

Current: ~333 records/second with 30-second micro-batches

**Increase throughput**:
```python
# Edit spark/jobs/kafka_to_iceberg_streaming.py
# Increase maxOffsetsPerTrigger
.option("maxOffsetsPerTrigger", 20000)  # Default: 10000
```

### Query Performance

**Use partition pruning**:
```sql
-- Good: Uses date partition
SELECT * FROM iceberg.bronze.transactions
WHERE date_partition = DATE '2025-11-17';

-- Bad: Full table scan
SELECT * FROM iceberg.bronze.transactions
WHERE timestamp > TIMESTAMP '2025-11-17 00:00:00';
```

**Check query plan**:
```sql
EXPLAIN SELECT * FROM iceberg.bronze.transactions
WHERE date_partition = DATE '2025-11-17';
```

### Storage Optimization

**Compact small files**:
```sql
-- Iceberg compaction (via Trino)
ALTER TABLE iceberg.bronze.transactions
EXECUTE optimize;
```

**Remove old snapshots**:
```sql
-- Expire snapshots older than 7 days
ALTER TABLE iceberg.bronze.transactions
EXECUTE expire_snapshots(retention_threshold => '7d');
```

---

## Stopping the System

### Stop Services (Keep Data)

```bash
# Stop all services
docker compose stop

# Restart later with:
docker compose start
```

### Stop and Remove Containers (Keep Volumes)

```bash
# Remove containers but keep data
docker compose down

# Restart with:
docker compose up -d
```

### Complete Cleanup (Delete All Data)

```bash
# WARNING: This deletes all data!
docker compose down -v

# This removes:
# - All containers
# - All volumes (MinIO data, PostgreSQL data, checkpoints)
# - Network configurations

# You'll need to re-run the entire workflow
```

---

## Next Steps

1. ✅ **Run the complete workflow** (Data generation → Query → Visualization)
2. ✅ **Create Superset dashboards** for your business metrics
3. ✅ **Explore Iceberg features** (time travel, schema evolution)
4. ✅ **Customize the data generator** for your use case
5. ✅ **Schedule batch jobs** (e.g., daily aggregations with cron)
6. ✅ **Add monitoring** (Prometheus + Grafana for metrics)
7. ✅ **Implement data quality checks** (Great Expectations, Deequ)

---

## Additional Resources

### Documentation

- **System Status**: [docs/SYSTEM_STATUS.md](docs/SYSTEM_STATUS.md)
- **Architecture**: [ARCHITECTURE.md](ARCHITECTURE.md)
- **Streaming Pipeline**: [docs/STREAMING_PIPELINE_FIX.md](docs/STREAMING_PIPELINE_FIX.md)
- **Batch Pipeline**: [docs/BATCH_AGGREGATIONS_UPDATE.md](docs/BATCH_AGGREGATIONS_UPDATE.md)
- **Superset Integration**: [docs/SUPERSET_TRINO_INTEGRATION.md](docs/SUPERSET_TRINO_INTEGRATION.md)

### Official Documentation

- **Apache Kafka**: https://kafka.apache.org/documentation/
- **Apache Spark**: https://spark.apache.org/docs/latest/
- **Apache Iceberg**: https://iceberg.apache.org/docs/latest/
- **Trino**: https://trino.io/docs/current/
- **Apache Superset**: https://superset.apache.org/docs/intro
- **MinIO**: https://min.io/docs/

### Key Commands Reference

```bash
# Data Generation
docker exec kafka-broker-1 python /scripts/data_generator.py --num-records 10000 --topic banking.transactions.raw

# Start Streaming
./submit-streaming-job.sh

# Run Batch Aggregations
./submit-batch-job.sh

# Trino CLI
docker exec -it trino trino --catalog iceberg --schema bronze

# Check Data Count
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.bronze.transactions;"

# View Logs
docker logs spark-master | tail -100
docker logs hive-metastore | tail -50
docker logs trino | tail -50

# Service Status
docker compose ps

# Restart Services
docker compose restart
```

---

## What Makes This Platform Different?

This is a **modern data lakehouse**, combining the best of data lakes and data warehouses:

✅ **Direct Query on Data Lake** - Query Parquet files directly with Trino, no ETL to warehouse

✅ **ACID Transactions** - Iceberg provides reliable updates, deletes, and concurrent writes

✅ **Time Travel** - Query historical snapshots without maintaining separate copies

✅ **Schema Evolution** - Add/modify columns without downtime or data rewrites

✅ **Open Formats** - Parquet + Iceberg = vendor-agnostic, portable data

✅ **Unified Architecture** - Single platform for batch and streaming workloads

✅ **Cost Effective** - Object storage (MinIO) is cheaper than traditional databases

✅ **Scalable** - Add Spark workers, Trino workers, or Kafka brokers as needed

---

**Ready to build your lakehouse? Start with Step 1!** 🚀
