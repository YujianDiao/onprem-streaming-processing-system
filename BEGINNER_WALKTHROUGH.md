# Complete Beginner's Walkthrough: From Zero to Data Analytics

This guide will walk you through every step of generating, streaming, processing, and querying data in the lakehouse platform. No prior Kafka or streaming experience needed!

## Table of Contents

1. [Understanding the Flow](#understanding-the-flow)
2. [Pre-Flight Checks](#pre-flight-checks)
3. [Step 1: Start the Platform](#step-1-start-the-platform)
4. [Step 2: Verify All Services](#step-2-verify-all-services)
5. [Step 3: Start Data Generation](#step-3-start-data-generation)
6. [Step 4: View Data in Kafka](#step-4-view-data-in-kafka)
7. [Step 5: Start Spark Processing](#step-5-start-spark-processing)
8. [Step 6: Query Data in Trino](#step-6-query-data-in-trino)
9. [Step 7: Monitor the Pipeline](#step-7-monitor-the-pipeline)
10. [Troubleshooting](#troubleshooting)
11. [What's Actually Happening?](#whats-actually-happening)

---

## Understanding the Flow

Before we start, let's understand what we're building:

```
┌─────────────────┐
│ Data Generator  │ Creates fake banking transactions (JSON)
└────────┬────────┘
         │ Sends via network
         ▼
┌─────────────────┐
│     Kafka       │ Receives and stores messages temporarily
└────────┬────────┘
         │ Streams continuously
         ▼
┌─────────────────┐
│  Spark Stream   │ Processes data every 30 seconds
└────────┬────────┘
         │ Writes to storage
         ▼
┌─────────────────┐
│ Iceberg Tables  │ Permanent storage in MinIO (like S3)
│   (in MinIO)    │
└────────┬────────┘
         │ Registered in
         ▼
┌─────────────────┐
│ Hive Metastore  │ Keeps track of table schemas
└────────┬────────┘
         │ Used by
         ▼
┌─────────────────┐
│     Trino       │ SQL engine for analytics
└─────────────────┘
```

**Think of it like a factory assembly line:**
- **Generator** = Raw materials coming in
- **Kafka** = Conveyor belt moving materials
- **Spark** = Processing machine that transforms materials
- **Iceberg/MinIO** = Warehouse storing finished products
- **Trino** = Inventory system to find and analyze products

---

## Pre-Flight Checks

Before starting, make sure you have:

```bash
# 1. Check Docker is running
docker --version
# Expected: Docker version 20.10.x or higher

# 2. Check Docker Compose
docker compose version
# Expected: Docker Compose version v2.x.x or higher

# 3. Check available resources
docker info | grep -E "CPUs|Total Memory"
# Expected: At least 8 CPUs and 12GB memory

# 4. Navigate to project directory
cd /home/user/onprem-streaming-processing-system
pwd
# Expected: /home/user/onprem-streaming-processing-system
```

---

## Step 1: Start the Platform

### 1.1 Start All Services

```bash
# This will start all containers (takes 2-3 minutes)
make start
```

**What happens:**
- Kafka brokers start (3 containers)
- PostgreSQL starts (for Hive Metastore)
- MinIO starts (object storage)
- Hive Metastore starts
- Spark cluster starts (1 master + 2 workers)
- Trino starts (SQL engine)
- Monitoring tools start (Prometheus, Grafana)

**Wait for this message:**
```
✓ All services started successfully
```

### 1.2 Alternative: Start Step-by-Step

If you prefer to see each component start:

```bash
# Step 1: Core infrastructure
docker compose up -d kafka-1 kafka-2 kafka-3 postgres minio

# Wait 30 seconds for Kafka to initialize
sleep 30

# Step 2: Metadata layer
docker compose up -d hive-metastore schema-registry

# Wait 20 seconds
sleep 20

# Step 3: Processing layer
docker compose up -d spark-master spark-worker-1 spark-worker-2

# Wait 15 seconds
sleep 15

# Step 4: Analytics layer
docker compose up -d trino

# Step 5: Monitoring
docker compose up -d kafka-ui prometheus grafana
```

---

## Step 2: Verify All Services

### 2.1 Check Container Status

```bash
make status
# Or: docker compose ps
```

**Expected output:**
```
NAME                STATUS              PORTS
kafka-1             running (healthy)   9092->9092
kafka-2             running (healthy)   9093->9093
kafka-3             running (healthy)   9094->9094
postgres            running (healthy)   5432->5432
minio               running (healthy)   9000-9001->9000-9001
hive-metastore      running (healthy)   9083->9083
spark-master        running (healthy)   7077, 8888->8888
spark-worker-1      running             8091->8091
spark-worker-2      running             8092->8092
trino               running (healthy)   8086->8086
kafka-ui            running             8080->8080
prometheus          running             9090->9090
grafana             running             3000->3000
```

**All containers should show "running"** or "running (healthy)".

### 2.2 Test Individual Services

```bash
# Test Kafka
make test-kafka
# Expected: ✓ Kafka cluster is healthy

# Test PostgreSQL (Hive Metastore backend)
make test-postgres
# Expected: ✓ PostgreSQL is accessible

# Test MinIO
make test-minio
# Expected: ✓ MinIO is running

# Test Trino
make test-trino
# Expected: ✓ Trino is ready
```

### 2.3 Open Web Interfaces

Open these in your browser to verify:

```bash
# Kafka UI - Monitor Kafka messages
open http://localhost:8080
# Or manually: http://localhost:8080

# Spark Master - Monitor processing jobs
open http://localhost:8888

# Trino - SQL interface
open http://localhost:8086

# MinIO - Object storage console
open http://localhost:9001
# Login: minioadmin / minioadmin
```

---

## Step 3: Start Data Generation

### 3.1 Understanding the Data Generator

The data generator creates **fake banking transactions** that look like this:

```json
{
  "transaction_id": "a7f3c821-4d5e-4a9b-8c7d-1e3f5a6b9c2d",
  "timestamp": "2024-01-15T10:30:45.123456Z",
  "account_id": "ACC1234",
  "transaction_type": "PURCHASE",
  "amount": 125.50,
  "currency": "USD",
  "merchant": "Amazon",
  "customer": {
    "customer_id": "CUST456",
    "name": "John Doe",
    "email": "john.doe@example.com"
  },
  "is_fraud": false
}
```

**Transaction types:**
- PURCHASE (45% of transactions)
- WITHDRAWAL (20%)
- DEPOSIT (15%)
- TRANSFER (15%)
- PAYMENT (5%)

**Generation rate:** 1 transaction per second by default

### 3.2 Start the Generator

```bash
# Start generating data
make datagen-start
# Or: docker compose up -d data-generator
```

**Expected output:**
```
✓ Data generator started
```

### 3.3 Watch the Generator in Action

```bash
# View live logs
make logs-datagen
# Or: docker compose logs -f data-generator
```

**You should see:**
```
Starting data generator...
Kafka Bootstrap Servers: ['kafka-1:29092', 'kafka-2:29093', 'kafka-3:29094']
Target Topic: banking.transactions.raw
Generation Interval: 1000ms
Connected to Kafka successfully!
Generated 10 transactions...
Generated 20 transactions...
Generated 30 transactions...
```

**Press Ctrl+C to stop viewing logs** (generator keeps running)

### 3.4 Verify Data is Being Sent

```bash
# Check how many messages the generator has sent
docker compose logs data-generator | grep "Generated"
```

**You should see increasing numbers:**
```
Generated 10 transactions...
Generated 20 transactions...
Generated 30 transactions...
```

---

## Step 4: View Data in Kafka

Now let's see if Kafka is receiving the data.

### 4.1 Using Kafka UI (Easiest)

1. **Open Kafka UI:** http://localhost:8080

2. **Click on "Topics"** in the left menu

3. **Click on "banking.transactions.raw"** topic

4. **Click on "Messages"** tab

5. **You should see real-time messages** appearing!

**What you'll see:**
- **Key**: Account ID (e.g., "ACC1234")
- **Value**: Full JSON transaction
- **Partition**: Which partition (0, 1, or 2)
- **Offset**: Message number in that partition
- **Timestamp**: When it arrived

### 4.2 Using Command Line

```bash
# View the last 5 messages
docker exec kafka-1 kafka-console-consumer \
  --topic banking.transactions.raw \
  --bootstrap-server kafka-1:29092 \
  --from-beginning \
  --max-messages 5
```

**Expected output:**
```json
{"transaction_id":"abc-123","account_id":"ACC1001","amount":99.50,...}
{"transaction_id":"def-456","account_id":"ACC2002","amount":250.00,...}
{"transaction_id":"ghi-789","account_id":"ACC3003","amount":75.25,...}
...
Processed a total of 5 messages
```

### 4.3 Check Topic Details

```bash
# See topic configuration
docker exec kafka-1 kafka-topics \
  --describe \
  --topic banking.transactions.raw \
  --bootstrap-server kafka-1:29092
```

**Expected output:**
```
Topic: banking.transactions.raw
PartitionCount: 3
ReplicationFactor: 2
Partition: 0    Leader: 1       Replicas: 1,2   Isr: 1,2
Partition: 1    Leader: 2       Replicas: 2,3   Isr: 2,3
Partition: 2    Leader: 3       Replicas: 3,1   Isr: 3,1
```

**What this means:**
- **3 partitions**: Data is split across 3 streams for parallel processing
- **Replication factor 2**: Each message is stored on 2 different brokers (fault tolerance)
- **Leader**: Which broker is responsible for this partition
- **Isr**: In-Sync Replicas (brokers that have up-to-date copies)

### 4.4 Monitor Message Rate

In Kafka UI (http://localhost:8080):
- Look at **"Messages/sec"** graph
- Should show ~1 message per second (or your configured rate)

---

## Step 5: Start Spark Processing

Now let's process the Kafka data and write it to the lakehouse!

### 5.1 Submit the Spark Streaming Job

```bash
# Navigate to spark jobs directory
cd /home/user/onprem-streaming-processing-system

# Submit the streaming job
docker exec spark-master spark-submit \
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
```

**This command does:**
1. Connects to Spark cluster
2. Loads Iceberg and Kafka libraries
3. Configures S3 connection to MinIO
4. Runs the streaming job

### 5.2 What You'll See

**Initial output:**
```
Starting Kafka to Iceberg streaming job
Connected to Kafka topic: banking.transactions.raw
Table local.bronze.transactions already exists (or) created successfully
Streaming query started - writing to Iceberg
```

**The job will keep running** - This is normal! It's a continuous streaming job.

**Note:** The job runs in the foreground. Open a **new terminal** for the next steps.

### 5.3 Monitor Spark Job

**Open a new terminal** and run:

```bash
# View Spark UI
open http://localhost:8888
```

**What to look for:**
1. **Running Applications** tab - Should show "KafkaToIcebergStreaming"
2. **Click on the application** to see details
3. **Streaming tab** - Shows micro-batch processing every 30 seconds
4. **Input Rate** - Shows records/second from Kafka
5. **Processing Time** - How long each batch takes

### 5.4 Check Spark Logs

In a new terminal:

```bash
# View Spark master logs
docker compose logs -f spark-master | grep -i "streaming\|batch\|iceberg"
```

**You should see messages like:**
```
Batch 1 processed: 30 records
Writing to Iceberg table: local.bronze.transactions
Batch 2 processed: 30 records
Batch 3 processed: 30 records
```

---

## Step 6: Query Data in Trino

Now let's query the data that Spark has written!

### 6.1 Wait for First Batch

**Wait 30-60 seconds** for Spark to process the first batch and write to Iceberg.

### 6.2 Connect to Trino

```bash
# Open Trino CLI
make shell-trino
# Or: docker exec -it trino trino
```

**You should see:**
```
trino>
```

This is the Trino SQL prompt. You can now run SQL queries!

### 6.3 Explore Catalogs and Schemas

```sql
-- See available catalogs
SHOW CATALOGS;
```

**Expected output:**
```
 Catalog
---------
 iceberg
 system
(2 rows)
```

**What this means:**
- **iceberg**: Your lakehouse data
- **system**: Trino system information

```sql
-- See schemas in iceberg catalog
SHOW SCHEMAS IN iceberg;
```

**Expected output:**
```
       Schema
--------------------
 bronze
 default
 information_schema
(3 rows)
```

**What this means:**
- **bronze**: Raw data layer (where our streaming job writes)
- **default**: Default schema
- **information_schema**: Metadata about tables

### 6.4 Check for Tables

```sql
-- List tables in bronze schema
SHOW TABLES IN iceberg.bronze;
```

**Expected output:**
```
    Table
--------------
 transactions
(1 row)
```

**If you don't see the table yet:**
- Wait another 30 seconds (first batch might still be processing)
- Check Spark logs to ensure job is running

### 6.5 Query Your First Data!

```sql
-- Count total records
SELECT COUNT(*) as total_records
FROM iceberg.bronze.transactions;
```

**Expected output:**
```
 total_records
---------------
           127
(1 row)
```

The number will increase as more data arrives!

### 6.6 View Sample Data

```sql
-- See the latest 10 transactions
SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    merchant,
    transaction_timestamp
FROM iceberg.bronze.transactions
ORDER BY transaction_timestamp DESC
LIMIT 10;
```

**Expected output:**
```
 transaction_id                       | account_id | transaction_type | amount  | merchant    | transaction_timestamp
--------------------------------------+------------+------------------+---------+-------------+------------------------
 a7f3c821-4d5e-4a9b-8c7d-1e3f5a6b9c2d | ACC1234    | PURCHASE         |  125.50 | Amazon      | 2024-01-15 10:30:45.123
 b8g4d932-5e6f-5b0c-9d8e-2f4g6b7c0d3e | ACC5678    | WITHDRAWAL       |  200.00 | NULL        | 2024-01-15 10:30:44.456
 ...
(10 rows)
```

### 6.7 Run Analytics Queries

**Total by transaction type:**

```sql
SELECT
    transaction_type,
    COUNT(*) as count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MAX(amount) as max_amount
FROM iceberg.bronze.transactions
GROUP BY transaction_type
ORDER BY total_amount DESC;
```

**Expected output:**
```
 transaction_type | count | total_amount | avg_amount | max_amount
------------------+-------+--------------+------------+------------
 PAYMENT          |    15 |    145230.50 |    9682.03 |   49500.00
 TRANSFER         |    42 |     98456.75 |    2344.21 |    9850.00
 PURCHASE         |   125 |     45678.90 |     365.43 |     498.50
 DEPOSIT          |    38 |     32109.25 |     844.98 |    4950.00
 WITHDRAWAL       |    52 |     21345.60 |     410.49 |     998.00
(5 rows)
```

**Transactions in the last minute:**

```sql
SELECT
    COUNT(*) as recent_count,
    SUM(amount) as recent_total
FROM iceberg.bronze.transactions
WHERE transaction_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' MINUTE;
```

**Find high-value transactions:**

```sql
SELECT
    transaction_id,
    account_id,
    amount,
    merchant,
    transaction_timestamp
FROM iceberg.bronze.transactions
WHERE amount > 10000
ORDER BY amount DESC
LIMIT 10;
```

### 6.8 Real-Time Verification

**Open two terminals side by side:**

**Terminal 1 - Keep querying:**
```sql
-- Run this every 10 seconds
SELECT COUNT(*) FROM iceberg.bronze.transactions;
```

**Terminal 2 - Watch generator:**
```bash
make logs-datagen
```

**You should see:**
- Generator producing ~1 transaction/second
- Count increasing every 30 seconds (when Spark writes a new batch)

---

## Step 7: Monitor the Pipeline

### 7.1 Complete Data Flow Check

**1. Generator → Kafka:**

```bash
# Terminal 1: Watch generator
docker compose logs -f data-generator | grep "Generated"

# Terminal 2: Check Kafka UI
open http://localhost:8080
# Navigate to Topics → banking.transactions.raw → Messages
```

**Verification:** Message count in Kafka UI should match generator count

**2. Kafka → Spark:**

```bash
# Open Spark UI
open http://localhost:8888
# Click on "Streaming" tab
# Check "Input Rate" graph
```

**Verification:** Input rate should be ~1 record/second

**3. Spark → Iceberg:**

```bash
# Watch Spark logs
docker compose logs -f spark-master | grep -i "batch processed"
```

**Verification:** Should see "Batch X processed" every 30 seconds

**4. Iceberg → Trino:**

```sql
-- In Trino CLI
SELECT
    MAX(transaction_timestamp) as latest_transaction,
    COUNT(*) as total_records,
    CURRENT_TIMESTAMP - MAX(transaction_timestamp) as data_lag
FROM iceberg.bronze.transactions;
```

**Verification:** Data lag should be < 1 minute

### 7.2 Monitor with Kafka UI

**Open:** http://localhost:8080

**Check these tabs:**

**Brokers:**
- All 3 brokers should be "Online"
- Check disk usage, memory

**Topics:**
- **banking.transactions.raw** should show:
  - Messages: Increasing count
  - Size: Growing bytes
  - Messages/sec: ~1/sec

**Consumers:**
- Should see Spark as a consumer
- **Consumer lag** should be < 100 (ideally close to 0)

**Consumer lag** = How far behind Spark is from Kafka
- **0-10**: Excellent, real-time processing
- **10-100**: Good, slight delay
- **>100**: Spark is falling behind

### 7.3 Monitor with Spark UI

**Open:** http://localhost:8888

**Streaming Tab:**

- **Input Rate**: Records/second from Kafka (~1/sec)
- **Processing Time**: How long each batch takes (should be < 30 sec)
- **Scheduling Delay**: Queued batches (should be 0)
- **Total Records**: Cumulative count

**If Processing Time > 30 seconds:**
- Spark is falling behind
- May need more workers or tuning

### 7.4 Monitor with Grafana (Optional)

**Open:** http://localhost:3000

**Login:** admin / admin

**Add Prometheus datasource:**
1. Configuration → Data Sources
2. Add data source → Prometheus
3. URL: `http://prometheus:9090`
4. Save & Test

**Create a dashboard** to monitor:
- Kafka message rate
- Spark batch processing time
- System CPU/Memory

---

## Troubleshooting

### Issue 1: Data Generator Not Starting

**Symptoms:**
```bash
docker compose ps data-generator
# Shows: Exit 1 or Restarting
```

**Solution:**
```bash
# Check logs
docker compose logs data-generator

# Common issue: Kafka not ready
# Wait 30 seconds and restart
docker compose restart data-generator
```

### Issue 2: No Messages in Kafka

**Check:**
```bash
# 1. Is generator running?
docker compose ps data-generator
# Should be: Up

# 2. Check generator logs for errors
docker compose logs data-generator | grep -i error

# 3. Verify Kafka topic exists
docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092
# Should include: banking.transactions.raw
```

**Fix:**
```bash
# Create topic manually if missing
docker exec kafka-1 kafka-topics --create \
  --topic banking.transactions.raw \
  --bootstrap-server kafka-1:29092 \
  --partitions 3 \
  --replication-factor 2
```

### Issue 3: Spark Job Failed

**Symptoms:**
```
Error: Connection refused to kafka-1:29092
Error: Access denied to s3a://lakehouse
```

**Solution:**
```bash
# Check Spark can reach Kafka
docker exec spark-master ping kafka-1 -c 2

# Check Spark can reach MinIO
docker exec spark-master curl http://minio:9000/minio/health/live
# Should return: OK

# Verify MinIO credentials
docker exec spark-master curl -I http://minio:9000
# Should return: 403 or 200
```

### Issue 4: No Tables in Trino

**Check:**
```sql
-- In Trino CLI
SHOW SCHEMAS IN iceberg;
-- Should show: bronze, default

-- If bronze doesn't exist
CREATE SCHEMA iceberg.bronze;
```

**Then wait 30 seconds** for Spark to create the table

### Issue 5: Data Not Updating

**Symptoms:** COUNT(*) doesn't increase

**Check pipeline:**

```bash
# 1. Generator running?
docker compose ps data-generator

# 2. Kafka receiving?
docker exec kafka-1 kafka-console-consumer \
  --topic banking.transactions.raw \
  --bootstrap-server kafka-1:29092 \
  --max-messages 1

# 3. Spark processing?
docker compose logs spark-master | tail -20

# 4. Check Spark UI
open http://localhost:8888
```

---

## What's Actually Happening?

Let's demystify what happens behind the scenes:

### The Complete Journey of One Transaction

**Step 1: Data Generation (0 seconds)**

```python
# data-generator/generator.py creates:
transaction = {
    "transaction_id": "abc-123",
    "account_id": "ACC1234",
    "amount": 99.50,
    ...
}

# Sends to Kafka
producer.send("banking.transactions.raw", key="ACC1234", value=transaction)
```

**Step 2: Kafka Receives (0.001 seconds)**

```
Kafka receives the message and:
1. Determines partition based on key hash(ACC1234) % 3 = partition 1
2. Appends message to partition 1 log
3. Replicates to another broker (fault tolerance)
4. Acknowledges to generator: "Message received!"
```

**Step 3: Spark Reads (every 30 seconds)**

```python
# Spark streaming query runs:
kafka_df = spark.readStream.format("kafka").load()

# Every 30 seconds, Spark:
# 1. Asks Kafka: "Give me all new messages since offset X"
# 2. Kafka returns batch (e.g., 30 messages)
# 3. Spark creates a DataFrame with these messages
```

**Step 4: Spark Transforms (within 30-second window)**

```python
# Parse JSON
parsed_df = kafka_df.select(from_json(col("value"), schema))

# Add metadata
enriched_df = parsed_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("date_partition", to_date(col("transaction_timestamp")))
```

**Step 5: Spark Writes to Iceberg (within 30-second window)**

```python
# Write with ACID guarantees
enriched_df.writeStream \
    .format("iceberg") \
    .option("path", "s3a://lakehouse/bronze/transactions") \
    .start()

# Behind the scenes:
# 1. Spark writes Parquet files to MinIO
# 2. Creates Iceberg metadata (manifest files)
# 3. Commits transaction atomically
# 4. Updates Hive Metastore
# 5. Saves checkpoint (for fault tolerance)
```

**Step 6: Storage in MinIO**

```
MinIO bucket structure:
s3://lakehouse/
  └── bronze/
      └── transactions/
          ├── data/
          │   ├── date_partition=2024-01-15/
          │   │   ├── 00000-0-abc123.parquet  (actual data)
          │   │   └── 00001-0-def456.parquet
          │   └── date_partition=2024-01-16/
          │       └── 00000-0-ghi789.parquet
          └── metadata/
              ├── v1.metadata.json  (table schema, partitions)
              ├── snap-123.avro     (snapshot info)
              └── manifest-list.avro (file locations)
```

**Step 7: Hive Metastore Tracks**

```sql
-- PostgreSQL stores:
- Table name: bronze.transactions
- Schema: columns and types
- Location: s3a://lakehouse/bronze/transactions
- Latest snapshot ID: 123456
- Partition info: date_partition column
```

**Step 8: Trino Queries**

```sql
-- When you run:
SELECT * FROM iceberg.bronze.transactions LIMIT 10;

-- Trino does:
1. Asks Hive Metastore: "Where is this table?"
2. Hive returns: "s3a://lakehouse/bronze/transactions"
3. Trino reads Iceberg metadata from MinIO
4. Metadata says: "Data is in these Parquet files: [...]"
5. Trino reads Parquet files directly from MinIO
6. Returns results to you
```

### Why This Architecture?

**Kafka = Buffer:**
- Handles spikes in data (if 1000 transactions arrive at once)
- Decouples producers from consumers
- Multiple consumers can read same data

**Spark = Processor:**
- Batches small messages into efficient writes
- Transforms and validates data
- Provides exactly-once guarantees

**Iceberg = Smart Storage:**
- ACID transactions (no partial writes)
- Time travel (query old versions)
- Schema evolution (change columns safely)
- Efficient queries (columnar Parquet format)

**Trino = Fast Analytics:**
- Queries Parquet files directly (no ETL needed)
- Distributed processing (scales to petabytes)
- Standard SQL (easy to learn)

---

## Next Steps

Now that you understand the basics, try:

### Experiment 1: Change Data Rate

```bash
# Stop current generator
docker compose stop data-generator

# Edit .env file
nano .env
# Change: GENERATION_INTERVAL_MS=500  (2 transactions/second)

# Restart generator
docker compose up -d data-generator

# Watch the impact in Spark UI
```

### Experiment 2: Create Your Own Tables

```sql
-- In Trino CLI
-- Create a Silver table with only valid transactions
CREATE TABLE iceberg.silver.valid_transactions AS
SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    merchant,
    transaction_timestamp
FROM iceberg.bronze.transactions
WHERE amount > 0 AND amount < 100000;

-- Query it
SELECT COUNT(*) FROM iceberg.silver.valid_transactions;
```

### Experiment 3: Advanced Analytics

```sql
-- Create hourly aggregates
CREATE TABLE iceberg.gold.hourly_stats AS
SELECT
    date_trunc('hour', transaction_timestamp) as hour,
    transaction_type,
    COUNT(*) as txn_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM iceberg.bronze.transactions
GROUP BY 1, 2
ORDER BY 1 DESC;

-- Query it
SELECT * FROM iceberg.gold.hourly_stats
ORDER BY hour DESC
LIMIT 20;
```

### Experiment 4: Time Travel

```sql
-- Note the current count
SELECT COUNT(*) as current FROM iceberg.bronze.transactions;

-- Note the time
SELECT CURRENT_TIMESTAMP;

-- Wait 2 minutes, then compare
SELECT COUNT(*) as old_count
FROM iceberg.bronze.transactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 10:30:00';  -- Use your noted time

-- See what was added
SELECT
    current.current - old.old_count as new_transactions
FROM
    (SELECT COUNT(*) as current FROM iceberg.bronze.transactions) current,
    (SELECT COUNT(*) as old_count FROM iceberg.bronze.transactions
     FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 10:30:00') old;
```

---

## Summary

You've successfully:

✅ Started the entire lakehouse platform
✅ Generated streaming data (1 transaction/second)
✅ Sent data to Kafka (3-broker cluster)
✅ Processed data with Spark (every 30 seconds)
✅ Stored data in Iceberg tables (in MinIO)
✅ Queried data with Trino (SQL analytics)
✅ Monitored the entire pipeline

**The data is flowing through your lakehouse in real-time!**

For more advanced topics, see:
- [DATA_STREAMING_GUIDE.md](DATA_STREAMING_GUIDE.md) - Deep dive into streaming
- [ARCHITECTURE.md](ARCHITECTURE.md) - System architecture
- [README.md](README.md) - Full documentation
