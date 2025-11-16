# Data Streaming and Processing Guide

Complete guide for streaming, processing data, and creating tables in the lakehouse platform.

## Table of Contents

1. [Overview](#overview)
2. [Data Streaming Pipeline](#data-streaming-pipeline)
3. [Stream Processing with Spark](#stream-processing-with-spark)
4. [Creating Tables in the Lakehouse](#creating-tables-in-the-lakehouse)
5. [Querying Data with Trino](#querying-data-with-trino)
6. [End-to-End Workflow](#end-to-end-workflow)
7. [Advanced Topics](#advanced-topics)

---

## Overview

The lakehouse platform uses a modern streaming architecture:

```
Data Source → Kafka (Event Stream) → Spark (Stream Processing) → Iceberg (Lakehouse) → Trino (SQL Analytics)
```

**Key Characteristics:**
- **Real-time ingestion**: Sub-second latency from source to Kafka
- **Continuous processing**: Spark processes micro-batches every 30 seconds
- **ACID guarantees**: Iceberg ensures transactional consistency
- **Direct SQL access**: Trino queries lakehouse without ETL

---

## Data Streaming Pipeline

### 1. Data Generation and Ingestion

#### A. Using the Built-in Data Generator

The platform includes a Python-based fake data generator that produces realistic banking transactions.

**Start the generator:**
```bash
make datagen-start
# Or: docker compose up -d data-generator
```

**Monitor data generation:**
```bash
make logs-datagen
# Or: docker compose logs -f data-generator
```

**Stop the generator:**
```bash
make datagen-stop
# Or: docker compose stop data-generator
```

**Generator Configuration:**

The generator is configured via environment variables (`.env` file):
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka-1:29092,kafka-2:29093,kafka-3:29094
KAFKA_TOPIC=banking.transactions.raw
GENERATION_INTERVAL_MS=1000  # Generate 1 transaction per second
```

**Sample Generated Transaction:**
```json
{
  "transaction_id": "a7f3c821-4d5e-4a9b-8c7d-1e3f5a6b9c2d",
  "timestamp": "2024-01-15T10:30:45.123456Z",
  "account_id": "ACC1234",
  "transaction_type": "PURCHASE",
  "amount": 125.50,
  "currency": "USD",
  "merchant": "Amazon",
  "location": {
    "city": "New York",
    "state": "NY",
    "country": "US",
    "latitude": 40.7128,
    "longitude": -74.0060
  },
  "customer": {
    "customer_id": "CUST456",
    "name": "John Doe",
    "email": "john.doe@example.com",
    "phone": "+1-555-0123"
  },
  "is_fraud": false,
  "status": "PENDING",
  "metadata": {
    "device_type": "mobile",
    "ip_address": "192.168.1.10",
    "session_id": "sess_abc123"
  }
}
```

#### B. Custom Data Ingestion

**Using Kafka Producer (Python):**

```python
from kafka import KafkaProducer
import json
from datetime import datetime

# Create producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  # Wait for all replicas
    retries=3
)

# Send transaction
transaction = {
    "transaction_id": "txn_001",
    "account_id": "ACC1001",
    "transaction_type": "PURCHASE",
    "amount": 99.99,
    "currency": "USD",
    "merchant": "Target",
    "transaction_timestamp": datetime.utcnow().isoformat(),
    "status": "PENDING",
    "metadata": {}
}

# Send to Kafka
producer.send('banking.transactions.raw', value=transaction, key=transaction['account_id'])
producer.flush()
```

**Using Kafka CLI:**

```bash
# Produce a single message
echo '{"transaction_id":"txn_002","account_id":"ACC1002","amount":50.00}' | \
  docker exec -i kafka-1 kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic banking.transactions.raw
```

### 2. Kafka Topics Configuration

**View existing topics:**
```bash
docker exec kafka-1 kafka-topics \
  --list \
  --bootstrap-server kafka-1:29092
```

**Create a new topic:**
```bash
docker exec kafka-1 kafka-topics \
  --create \
  --bootstrap-server kafka-1:29092 \
  --topic banking.transactions.raw \
  --partitions 3 \
  --replication-factor 2 \
  --config retention.ms=604800000 \
  --config compression.type=snappy
```

**Topic Configuration:**
- **Partitions**: 3 (for parallel processing)
- **Replication Factor**: 2 (for fault tolerance)
- **Retention**: 7 days (604800000 ms)
- **Compression**: Snappy (balanced speed/size)

**Describe topic details:**
```bash
docker exec kafka-1 kafka-topics \
  --describe \
  --topic banking.transactions.raw \
  --bootstrap-server kafka-1:29092
```

### 3. Monitoring Kafka Streams

**Via Kafka UI (http://localhost:8080):**
- Navigate to Topics → `banking.transactions.raw`
- View message count, throughput, partitions
- Browse recent messages
- Monitor consumer lag

**Via CLI - Consume messages:**
```bash
docker exec kafka-1 kafka-console-consumer \
  --topic banking.transactions.raw \
  --bootstrap-server kafka-1:29092 \
  --from-beginning \
  --max-messages 10
```

**Check consumer groups:**
```bash
docker exec kafka-1 kafka-consumer-groups \
  --list \
  --bootstrap-server kafka-1:29092
```

---

## Stream Processing with Spark

### 1. Spark Streaming Architecture

The platform uses **Spark Structured Streaming** to continuously process data from Kafka and write to Iceberg tables.

**Key Components:**
- **Spark Master** (spark-master:7077) - Coordinates cluster
- **Spark Workers** (spark-worker-1, spark-worker-2) - Execute tasks
- **Streaming Job** (kafka_to_iceberg_streaming.py) - Processes data

**Access Spark UI:**
```bash
# Open Spark Master UI
open http://localhost:8888

# Check running applications
docker exec spark-master curl -s http://localhost:8080/json/ | jq '.activeapps'
```

### 2. Understanding the Streaming Job

The streaming job is defined in `spark/jobs/kafka_to_iceberg_streaming.py`:

**Job Flow:**
```
1. Connect to Kafka
   ↓
2. Read streaming data (micro-batches)
   ↓
3. Parse JSON and apply schema
   ↓
4. Add processing metadata
   ↓
5. Create Iceberg table (if not exists)
   ↓
6. Write to Iceberg with ACID guarantees
   ↓
7. Update checkpoints for fault tolerance
```

**Configuration Details:**

```python
# Kafka Source Configuration
spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29093,kafka-3:29094") \
    .option("subscribe", "banking.transactions.raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Iceberg Sink Configuration
enriched_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", "s3a://lakehouse/bronze/transactions") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze_transactions") \
    .option("fanout-enabled", "true") \
    .trigger(processingTime='30 seconds') \
    .start()
```

**Key Parameters:**
- **Processing Time**: 30 seconds (micro-batch interval)
- **Starting Offsets**: Latest (start from newest messages)
- **Output Mode**: Append (only add new records)
- **Checkpoint Location**: S3A path for fault recovery
- **Fanout**: Enabled for better write performance

### 3. Submit Custom Spark Jobs

**Submit a streaming job:**

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://lakehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /opt/bitnami/spark/jobs/kafka_to_iceberg_streaming.py
```

**Submit a batch job for aggregations:**

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 \
  /opt/bitnami/spark/jobs/batch_aggregations.py
```

### 4. Interactive Spark Shell

**Start Spark Shell with Iceberg:**

```bash
docker exec -it spark-master spark-shell \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://lakehouse/
```

**Example Scala commands:**

```scala
// Read from Iceberg table
val df = spark.table("local.bronze.transactions")
df.show(10)

// Count records
df.count()

// Filter and aggregate
df.filter($"amount" > 1000)
  .groupBy("transaction_type")
  .count()
  .show()
```

**PySpark Shell:**

```bash
docker exec -it spark-master pyspark \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2
```

---

## Creating Tables in the Lakehouse

### 1. Automatic Table Creation (via Spark Streaming)

Tables are automatically created by the Spark streaming job when it first processes data.

**Schema Definition:**

From `spark/jobs/kafka_to_iceberg_streaming.py`:

```python
# Transaction schema
StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("amount", DecimalType(18, 2), False),
    StructField("currency", StringType(), False),
    StructField("merchant", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("metadata", MapType(StringType(), StringType()), True)
])
```

**Automatic Table Creation SQL:**

```sql
CREATE TABLE IF NOT EXISTS local.bronze.transactions (
    transaction_id STRING,
    account_id STRING,
    transaction_type STRING,
    amount DECIMAL(18,2),
    currency STRING,
    merchant STRING,
    merchant_category STRING,
    transaction_timestamp TIMESTAMP,
    status STRING,
    metadata MAP<STRING, STRING>,
    ingestion_timestamp TIMESTAMP,
    date_partition DATE
)
USING iceberg
PARTITIONED BY (days(date_partition))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.metadata.compression-codec' = 'gzip'
)
```

**Table Properties:**
- **Format**: Parquet (columnar storage)
- **Partitioning**: Daily partitions based on `date_partition`
- **Metadata Compression**: GZIP
- **Location**: `s3a://lakehouse/bronze/transactions`

### 2. Manual Table Creation (via Trino)

**Connect to Trino CLI:**

```bash
make shell-trino
# Or: docker exec -it trino trino
```

**Create a database/schema:**

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.silver;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;
```

**Create an Iceberg table from scratch:**

```sql
CREATE TABLE iceberg.silver.daily_transactions (
    transaction_date DATE,
    account_id VARCHAR,
    total_transactions BIGINT,
    total_amount DECIMAL(18,2),
    avg_amount DECIMAL(18,2),
    max_amount DECIMAL(18,2),
    fraud_count BIGINT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['transaction_date'],
    location = 's3a://lakehouse/silver/daily_transactions'
);
```

**Create table from query (CTAS):**

```sql
CREATE TABLE iceberg.gold.high_value_transactions
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['date_partition']
)
AS
SELECT
    transaction_id,
    account_id,
    amount,
    transaction_timestamp,
    date_partition
FROM iceberg.bronze.transactions
WHERE amount > 10000;
```

**Create partitioned table:**

```sql
CREATE TABLE iceberg.silver.transactions_by_type (
    transaction_id VARCHAR,
    transaction_type VARCHAR,
    amount DECIMAL(18,2),
    transaction_timestamp TIMESTAMP,
    year INTEGER,
    month INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['transaction_type', 'year', 'month']
);
```

### 3. Table Organization (Medallion Architecture)

The platform follows a **medallion architecture** for data organization:

```
Bronze Layer (Raw Data)
    ↓
Silver Layer (Cleaned/Enriched)
    ↓
Gold Layer (Business Aggregates)
```

**Bronze Tables** (`iceberg.bronze.*`):
- Raw data from sources
- Minimal transformations
- Includes ingestion metadata
- Example: `bronze.transactions`

**Silver Tables** (`iceberg.silver.*`):
- Cleaned and validated data
- Business rules applied
- Deduplicated
- Example: `silver.validated_transactions`

**Gold Tables** (`iceberg.gold.*`):
- Business-level aggregates
- Pre-computed metrics
- Optimized for BI tools
- Example: `gold.daily_revenue_by_merchant`

**Create Silver table from Bronze:**

```sql
CREATE TABLE iceberg.silver.validated_transactions AS
SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    currency,
    merchant,
    transaction_timestamp,
    status
FROM iceberg.bronze.transactions
WHERE status != 'FAILED'
  AND amount > 0
  AND transaction_timestamp IS NOT NULL;
```

**Create Gold aggregate table:**

```sql
CREATE TABLE iceberg.gold.hourly_metrics AS
SELECT
    date_trunc('hour', transaction_timestamp) as hour,
    transaction_type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    COUNT(DISTINCT account_id) as unique_accounts
FROM iceberg.silver.validated_transactions
GROUP BY 1, 2;
```

### 4. Schema Evolution

Iceberg supports schema evolution without table rewrites:

**Add a new column:**

```sql
ALTER TABLE iceberg.bronze.transactions
ADD COLUMN risk_score DOUBLE;
```

**Rename a column:**

```sql
ALTER TABLE iceberg.bronze.transactions
RENAME COLUMN merchant TO merchant_name;
```

**Drop a column:**

```sql
ALTER TABLE iceberg.bronze.transactions
DROP COLUMN metadata;
```

**Change column type (if compatible):**

```sql
ALTER TABLE iceberg.bronze.transactions
ALTER COLUMN amount SET DATA TYPE DECIMAL(20,4);
```

### 5. Partition Evolution

Change partitioning strategy without rewriting data:

**Add new partition:**

```sql
ALTER TABLE iceberg.bronze.transactions
ADD PARTITION FIELD bucket(account_id, 10);
```

**Drop partition:**

```sql
ALTER TABLE iceberg.bronze.transactions
DROP PARTITION FIELD days(date_partition);
```

---

## Querying Data with Trino

### 1. Basic Queries

**Connect to Trino:**

```bash
docker exec -it trino trino
```

**Show catalogs and schemas:**

```sql
SHOW CATALOGS;
-- Output: iceberg, system

SHOW SCHEMAS IN iceberg;
-- Output: bronze, silver, gold, default

SHOW TABLES IN iceberg.bronze;
-- Output: transactions
```

**Simple SELECT queries:**

```sql
-- Get first 10 transactions
SELECT * FROM iceberg.bronze.transactions
LIMIT 10;

-- Count total transactions
SELECT COUNT(*) as total_count
FROM iceberg.bronze.transactions;

-- Get latest transactions
SELECT
    transaction_id,
    account_id,
    amount,
    transaction_timestamp
FROM iceberg.bronze.transactions
ORDER BY transaction_timestamp DESC
LIMIT 20;
```

### 2. Analytical Queries

**Aggregate by transaction type:**

```sql
SELECT
    transaction_type,
    COUNT(*) as count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM iceberg.bronze.transactions
GROUP BY transaction_type
ORDER BY total_amount DESC;
```

**Daily revenue analysis:**

```sql
SELECT
    date_partition,
    COUNT(*) as transaction_count,
    SUM(amount) as daily_revenue,
    AVG(amount) as avg_transaction_value,
    COUNT(DISTINCT account_id) as unique_accounts
FROM iceberg.bronze.transactions
WHERE date_partition >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY date_partition
ORDER BY date_partition DESC;
```

**Fraud detection:**

```sql
SELECT
    account_id,
    COUNT(*) as suspicious_count,
    SUM(amount) as total_suspicious_amount,
    ARRAY_AGG(transaction_id) as transaction_ids
FROM iceberg.bronze.transactions
WHERE amount > 10000
  AND transaction_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY account_id
HAVING COUNT(*) > 3
ORDER BY total_suspicious_amount DESC;
```

### 3. Time Travel Queries (Iceberg Feature)

**View table snapshots:**

```sql
SELECT
    snapshot_id,
    parent_id,
    operation,
    manifest_list,
    committed_at
FROM iceberg.bronze.transactions$snapshots
ORDER BY committed_at DESC;
```

**Query historical data:**

```sql
-- Query as of specific timestamp
SELECT COUNT(*)
FROM iceberg.bronze.transactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 10:00:00';

-- Query specific snapshot
SELECT *
FROM iceberg.bronze.transactions
FOR SYSTEM_VERSION AS OF 1234567890
LIMIT 10;
```

**Compare data across time:**

```sql
WITH current_data AS (
    SELECT COUNT(*) as current_count
    FROM iceberg.bronze.transactions
),
historical_data AS (
    SELECT COUNT(*) as historical_count
    FROM iceberg.bronze.transactions
    FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 00:00:00'
)
SELECT
    current_count,
    historical_count,
    current_count - historical_count as new_transactions
FROM current_data, historical_data;
```

### 4. Metadata Queries

**View table files:**

```sql
SELECT
    file_path,
    file_format,
    record_count,
    file_size_in_bytes / 1024 / 1024 as size_mb,
    value_counts,
    null_value_counts
FROM iceberg.bronze.transactions$files
ORDER BY record_count DESC
LIMIT 10;
```

**View partitions:**

```sql
SELECT
    partition,
    record_count,
    file_count
FROM iceberg.bronze.transactions$partitions
ORDER BY partition DESC;
```

**View table manifests:**

```sql
SELECT
    path,
    length,
    partition_spec_id,
    added_snapshot_id
FROM iceberg.bronze.transactions$manifests
ORDER BY added_snapshot_id DESC;
```

**Table properties:**

```sql
SHOW CREATE TABLE iceberg.bronze.transactions;
```

### 5. Performance Optimization Queries

**Analyze data freshness:**

```sql
SELECT
    MAX(transaction_timestamp) as latest_transaction,
    CURRENT_TIMESTAMP as current_time,
    CURRENT_TIMESTAMP - MAX(transaction_timestamp) as data_lag
FROM iceberg.bronze.transactions;
```

**Check partition distribution:**

```sql
SELECT
    date_partition,
    COUNT(*) as record_count,
    SUM(amount) as partition_total
FROM iceberg.bronze.transactions
GROUP BY date_partition
ORDER BY date_partition DESC
LIMIT 30;
```

**Identify large partitions:**

```sql
SELECT
    partition,
    record_count,
    file_count,
    file_count * 1.0 / NULLIF(record_count, 0) as files_per_record
FROM iceberg.bronze.transactions$partitions
WHERE record_count > 1000000
ORDER BY record_count DESC;
```

---

## End-to-End Workflow

### Complete Example: From Stream to Analytics

**Step 1: Start Data Generation**

```bash
# Start the data generator
make datagen-start

# Verify data is flowing to Kafka
docker exec kafka-1 kafka-console-consumer \
  --topic banking.transactions.raw \
  --bootstrap-server kafka-1:29092 \
  --max-messages 5
```

**Step 2: Verify Spark Processing**

```bash
# Check Spark UI for running jobs
open http://localhost:8888

# View Spark logs
docker compose logs -f spark-master | grep -i streaming
```

**Step 3: Create Tables in Trino**

```bash
# Connect to Trino
docker exec -it trino trino
```

```sql
-- Wait for Bronze table to be auto-created by Spark
SHOW TABLES IN iceberg.bronze;

-- Create Silver layer (cleaned data)
CREATE SCHEMA IF NOT EXISTS iceberg.silver;

CREATE TABLE iceberg.silver.transactions AS
SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    currency,
    merchant,
    transaction_timestamp,
    date_partition
FROM iceberg.bronze.transactions
WHERE status = 'PENDING'
  AND amount > 0;

-- Create Gold layer (aggregates)
CREATE SCHEMA IF NOT EXISTS iceberg.gold;

CREATE TABLE iceberg.gold.daily_summary AS
SELECT
    date_partition,
    transaction_type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM iceberg.silver.transactions
GROUP BY date_partition, transaction_type;
```

**Step 4: Run Analytics Queries**

```sql
-- Top merchants by revenue
SELECT
    merchant,
    COUNT(*) as transactions,
    SUM(amount) as revenue
FROM iceberg.silver.transactions
WHERE merchant IS NOT NULL
GROUP BY merchant
ORDER BY revenue DESC
LIMIT 10;

-- Hourly transaction patterns
SELECT
    date_trunc('hour', transaction_timestamp) as hour,
    COUNT(*) as txn_count,
    SUM(amount) as hourly_revenue
FROM iceberg.silver.transactions
WHERE date_partition = CURRENT_DATE
GROUP BY 1
ORDER BY 1;
```

**Step 5: Monitor System**

```bash
# Open Grafana for metrics
open http://localhost:3000

# Check Kafka lag
open http://localhost:8080

# View MinIO storage
open http://localhost:9001
```

---

## Advanced Topics

### 1. Continuous Aggregations with Spark

Create materialized views using Spark streaming:

```python
# Read from Bronze table
bronze_stream = spark.readStream.table("local.bronze.transactions")

# Aggregate and write to Gold table
aggregated = bronze_stream \
    .groupBy(
        window("transaction_timestamp", "1 hour"),
        "transaction_type"
    ) \
    .agg(
        count("*").alias("count"),
        sum("amount").alias("total_amount")
    )

aggregated.writeStream \
    .format("iceberg") \
    .outputMode("complete") \
    .option("path", "s3a://lakehouse/gold/hourly_aggregates") \
    .trigger(processingTime='5 minutes') \
    .start()
```

### 2. Multi-Topic Processing

Process multiple Kafka topics in a single job:

```python
# Read from multiple topics
df1 = spark.readStream.format("kafka") \
    .option("subscribe", "banking.transactions.raw,banking.accounts.cdc") \
    .load()

# Route to different tables based on topic
df1.writeStream \
    .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id)) \
    .start()
```

### 3. Data Quality Checks

Implement data validation before writing to Silver:

```sql
CREATE TABLE iceberg.silver.validated_transactions AS
SELECT *
FROM iceberg.bronze.transactions
WHERE
    transaction_id IS NOT NULL
    AND account_id IS NOT NULL
    AND amount > 0
    AND amount < 1000000
    AND transaction_timestamp >= TIMESTAMP '2024-01-01'
    AND transaction_timestamp <= CURRENT_TIMESTAMP
    AND transaction_type IN ('PURCHASE', 'WITHDRAWAL', 'DEPOSIT', 'TRANSFER', 'PAYMENT');
```

### 4. Incremental Updates

Update Gold tables incrementally:

```sql
INSERT INTO iceberg.gold.daily_summary
SELECT
    date_partition,
    transaction_type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM iceberg.silver.transactions
WHERE date_partition = CURRENT_DATE
GROUP BY date_partition, transaction_type;
```

### 5. Table Maintenance

**Optimize table files:**

```sql
-- Compact small files (Trino)
ALTER TABLE iceberg.bronze.transactions EXECUTE optimize;
```

**Expire old snapshots:**

```sql
-- Remove snapshots older than 7 days
ALTER TABLE iceberg.bronze.transactions
EXECUTE expire_snapshots(retention_threshold => '7d');
```

**Remove orphan files:**

```sql
-- Clean up unreferenced files
ALTER TABLE iceberg.bronze.transactions
EXECUTE remove_orphan_files(older_than => '3d');
```

### 6. Performance Tuning

**Optimize Spark streaming:**

```python
# Increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")

# Tune batch interval
.trigger(processingTime='10 seconds')  # Faster processing

# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

**Optimize Trino queries:**

```sql
-- Use partition pruning
SELECT * FROM iceberg.bronze.transactions
WHERE date_partition >= DATE '2024-01-01'
  AND date_partition < DATE '2024-02-01';

-- Predicate pushdown
SELECT * FROM iceberg.bronze.transactions
WHERE amount > 1000  -- Pushed to Parquet reader
LIMIT 100;
```

---

## Summary

This guide covers the complete data pipeline:

1. **Streaming**: Generate or produce data to Kafka topics
2. **Processing**: Spark consumes Kafka streams and writes to Iceberg
3. **Storage**: Data stored in MinIO as Iceberg tables (Parquet format)
4. **Analytics**: Trino queries Iceberg tables directly with SQL

**Key Takeaways:**

- **No ETL needed**: Trino queries the lakehouse directly
- **ACID guarantees**: Iceberg provides transactional consistency
- **Time travel**: Query historical data at any point in time
- **Schema evolution**: Modify schemas without downtime
- **Continuous processing**: Spark streaming runs 24/7
- **Scalable**: Add more Kafka brokers, Spark workers, or Trino nodes

**Next Steps:**

- Experiment with custom Spark jobs
- Create your own data models (Bronze/Silver/Gold)
- Build real-time dashboards with Grafana
- Optimize performance for your use case
- Implement data quality checks

For more information, see:
- [ARCHITECTURE.md](ARCHITECTURE.md) - Detailed architecture
- [README.md](README.md) - Platform overview
- [QUICK_START.md](QUICK_START.md) - Quick setup guide
