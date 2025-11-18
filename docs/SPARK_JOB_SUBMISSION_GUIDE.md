# Spark Streaming Job Submission Guide

## Issues Fixed

### 1. ✅ Incorrect spark-submit Path
**Problem:** Command tried to use `spark-submit` directly, but it's not in PATH
**Solution:** Use full path: `/opt/spark/bin/spark-submit`

### 2. ✅ Wrong Job Path
**Problem:** Command used `/opt/bitnami/spark/jobs/` (Bitnami path)
**Solution:** Use `/opt/spark/jobs/` (Apache Spark path)

### 3. ✅ Incorrect Kafka Bootstrap Servers
**Problem:** Job used ports 29093, 29094 for brokers 2 and 3
**Solution:** All brokers use internal port 29092

### 4. ✅ Schema Mismatch
**Problem:** Spark schema didn't match actual transaction data
**Solution:** Updated schema to include all fields (location, customer, is_fraud, metadata)

---

## Quick Start - Submit the Job

### Option 1: Use the Script (Easiest)

```bash
./submit-streaming-job.sh
```

### Option 2: Run Command Directly

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
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
  /opt/spark/jobs/kafka_to_iceberg_streaming.py
```

---

## What the Job Does

### Data Flow

```
Kafka Topic: banking.transactions.raw
     ↓
Spark Structured Streaming
     ↓
Parse JSON & Apply Schema
     ↓
Add Metadata (ingestion_timestamp, date_partition)
     ↓
Write to Iceberg Table
     ↓
MinIO: s3a://lakehouse/bronze/transactions
```

### Processing Details

- **Source:** Kafka topic `banking.transactions.raw`
- **Bootstrap Servers:** kafka-1:29092, kafka-2:29092, kafka-3:29092
- **Starting Offset:** Latest (only new messages)
- **Trigger:** Every 30 seconds
- **Output:** Iceberg table `local.bronze.transactions`
- **Format:** Parquet with Gzip compression
- **Partitioning:** By date (daily partitions)

---

## Job Configuration

### Spark Packages (Auto-downloaded)

1. **iceberg-spark-runtime-3.5_2.12:1.4.2**
   - Iceberg integration with Spark 3.5

2. **spark-sql-kafka-0-10_2.12:3.5.0**
   - Kafka connector for Spark Structured Streaming

3. **hadoop-aws:3.3.4**
   - S3A filesystem support for MinIO

4. **aws-java-sdk-bundle:1.12.262**
   - AWS SDK for S3 operations

### Spark Configurations

| Config | Value | Purpose |
|--------|-------|---------|
| spark.sql.extensions | IcebergSparkSessionExtensions | Enable Iceberg SQL |
| spark.sql.catalog.local.type | hadoop | Use Hadoop catalog |
| spark.sql.catalog.local.warehouse | s3a://lakehouse/ | Iceberg warehouse location |
| spark.hadoop.fs.s3a.endpoint | http://minio:9000 | MinIO endpoint |
| spark.hadoop.fs.s3a.access.key | minioadmin | MinIO access key |
| spark.hadoop.fs.s3a.secret.key | minioadmin | MinIO secret key |
| spark.hadoop.fs.s3a.path.style.access | true | Use path-style access for MinIO |

---

## Transaction Schema

The job expects JSON data with this structure:

```json
{
  "transaction_id": "string",
  "timestamp": "ISO8601 string",
  "account_id": "string",
  "transaction_type": "PURCHASE|WITHDRAWAL|DEPOSIT|TRANSFER|PAYMENT",
  "amount": 123.45,
  "currency": "USD",
  "merchant": "string (nullable)",
  "location": {
    "city": "string",
    "state": "string",
    "country": "string",
    "latitude": 12.345,
    "longitude": -67.890
  },
  "customer": {
    "customer_id": "string",
    "name": "string",
    "email": "string",
    "phone": "string"
  },
  "is_fraud": true|false,
  "status": "PENDING|APPROVED|DECLINED",
  "metadata": {
    "device_type": "mobile|web|atm|pos",
    "ip_address": "string",
    "session_id": "string"
  }
}
```

---

## Iceberg Table Structure

### Table: `local.bronze.transactions`

```sql
CREATE TABLE local.bronze.transactions (
    transaction_id STRING,
    timestamp STRING,
    account_id STRING,
    transaction_type STRING,
    amount DOUBLE,
    currency STRING,
    merchant STRING,
    location STRUCT<city:STRING,state:STRING,country:STRING,latitude:DOUBLE,longitude:DOUBLE>,
    customer STRUCT<customer_id:STRING,name:STRING,email:STRING,phone:STRING>,
    is_fraud BOOLEAN,
    status STRING,
    metadata STRUCT<device_type:STRING,ip_address:STRING,session_id:STRING>,
    ingestion_timestamp TIMESTAMP,
    date_partition DATE
)
USING iceberg
PARTITIONED BY (days(date_partition))
```

### Storage Location

- **Path:** `s3a://lakehouse/bronze/transactions`
- **Format:** Parquet
- **Compression:** Gzip
- **Checkpoints:** `s3a://lakehouse/checkpoints/bronze_transactions`

---

## Monitoring the Job

### Check Job Status in Spark UI

```bash
# Open Spark Master UI
http://localhost:8888

# Look for:
# - Running Applications
# - Application Name: KafkaToIcebergStreaming
# - Status: RUNNING
```

### Check Job Logs

```bash
# View streaming output
docker logs spark-master -f

# Check for messages like:
# "Connected to Kafka topic: banking.transactions.raw"
# "Streaming query started - writing to Iceberg"
```

### Verify Data in Iceberg

Using Trino:

```sql
-- Check table exists
SHOW TABLES IN iceberg.bronze;

-- Count records
SELECT COUNT(*) FROM iceberg.bronze.transactions;

-- View recent transactions
SELECT
    transaction_id,
    timestamp,
    account_id,
    transaction_type,
    amount,
    is_fraud,
    ingestion_timestamp
FROM iceberg.bronze.transactions
ORDER BY ingestion_timestamp DESC
LIMIT 10;

-- Check partitions
SELECT
    date_partition,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount
FROM iceberg.bronze.transactions
GROUP BY date_partition
ORDER BY date_partition DESC;
```

---

## Troubleshooting

### Issue: Package Download Timeout

**Symptom:** Job hangs downloading packages

**Solution:**
```bash
# Download packages manually first
docker exec spark-master /opt/spark/bin/spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262

# Press Ctrl+C after packages download, then submit job
```

### Issue: Unable to Connect to Kafka

**Symptom:** `Connection refused` or `Couldn't resolve broker`

**Check:**
```bash
# Verify Kafka is running
docker compose ps kafka-1 kafka-2 kafka-3

# Test from spark-master
docker exec spark-master nc -zv kafka-1 29092
docker exec spark-master nc -zv kafka-2 29092
docker exec spark-master nc -zv kafka-3 29092
```

### Issue: Unable to Write to MinIO

**Symptom:** `AccessDenied` or `Connection refused to MinIO`

**Check:**
```bash
# Verify MinIO is accessible
docker exec spark-master curl -I http://minio:9000

# Check credentials
docker exec minio printenv | grep MINIO_ROOT
```

### Issue: Schema Mismatch

**Symptom:** `Unable to infer schema` or `Column not found`

**Fix:**
Check actual data format:
```bash
docker exec kafka-1 kafka-console-consumer \
  --topic banking.transactions.raw \
  --partition 1 \
  --bootstrap-server localhost:9092 \
  --max-messages 1 | jq '.'
```

### Issue: Job Exits Immediately

**Symptom:** Job starts but exits without error

**Check:**
```bash
# View full logs
docker logs spark-master 2>&1 | tail -100

# Check if topic has data
docker exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic banking.transactions.raw
```

---

## Stopping the Job

The streaming job runs indefinitely. To stop it:

### Method 1: Kill from Spark UI
1. Go to http://localhost:8888
2. Click on the application
3. Click "Kill" button

### Method 2: Kill via Command
```bash
# Find the application ID
docker exec spark-master /opt/spark/bin/spark-submit --status \
  --master spark://spark-master:7077 \
  --kill <application-id>
```

### Method 3: Restart Spark Master (Nuclear option)
```bash
docker compose restart spark-master
```

---

## Files Modified

1. **spark/jobs/kafka_to_iceberg_streaming.py**
   - Fixed Kafka bootstrap servers (all use port 29092)
   - Updated schema to match actual transaction data
   - Added location, customer, is_fraud, metadata fields
   - Fixed timestamp field name

2. **submit-streaming-job.sh** (Created)
   - Convenience script for job submission
   - Correct paths for Apache Spark

---

## Next Steps

1. ✅ **Submit the job:**
   ```bash
   ./submit-streaming-job.sh
   ```

2. ✅ **Verify it's running:**
   ```bash
   # Check Spark UI
   http://localhost:8888
   ```

3. ✅ **Wait 1-2 minutes** for first micro-batch

4. ✅ **Query data in Trino:**
   ```bash
   docker exec -it trino trino
   SELECT COUNT(*) FROM iceberg.bronze.transactions;
   ```

5. ✅ **Monitor continuously:**
   ```bash
   # Watch logs
   docker logs spark-master -f
   ```

---

**Status:** ✅ Job is ready to submit with correct paths, schema, and configuration!
