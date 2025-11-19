# Hands-On Tutorial: Data Lakehouse Platform PoC

**Purpose:** Get hands-on experience with the streaming data lakehouse platform
**Duration:** 2-4 hours
**Difficulty:** Beginner to Advanced
**Prerequisites:** Docker, basic SQL knowledge

---

## Table of Contents

1. [Platform Setup](#platform-setup)
2. [Quick Start - Your First 5 Minutes](#quick-start---your-first-5-minutes)
3. [Use Case 1: Data Analyst - Exploring Transaction Data](#use-case-1-data-analyst---exploring-transaction-data)
4. [Use Case 2: Business Analyst - Creating Dashboards](#use-case-2-business-analyst---creating-dashboards)
5. [Use Case 3: Data Engineer - Building ETL Pipelines](#use-case-3-data-engineer---building-etl-pipelines)
6. [Use Case 4: Real-Time Monitoring - Streaming Data](#use-case-4-real-time-monitoring---streaming-data)
7. [Use Case 5: Data Quality Engineer - Validation & Testing](#use-case-5-data-quality-engineer---validation--testing)
8. [Use Case 6: Platform Engineer - Time Travel & Schema Evolution](#use-case-6-platform-engineer---time-travel--schema-evolution)
9. [Use Case 7: Performance Tuning - Optimization](#use-case-7-performance-tuning---optimization)
10. [Troubleshooting & Experimentation](#troubleshooting--experimentation)

---

## Platform Setup

### Step 1: Start the Platform

```bash
# Clone repository (if not already done)
git clone https://github.com/YujianDiao/onprem-streaming-processing-system.git
cd onprem-streaming-processing-system

# Start all services
make start

# Wait for services to be ready (2-3 minutes)
make status

# Expected output:
# ✅ Kafka brokers running
# ✅ Spark cluster running
# ✅ Trino running
# ✅ MinIO running
# ✅ PostgreSQL running
```

### Step 2: Verify Platform Health

```bash
# Check all services
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# You should see:
# - kafka1, kafka2, kafka3 (Up)
# - spark-master, spark-worker-1, spark-worker-2 (Up)
# - trino, minio, postgres, hive-metastore (Up)
# - prometheus, grafana, kafka-ui (Up)
```

### Step 3: Create Topics and Start Data Generation

```bash
# Create Kafka topics
make topics

# Start data generator (generates 1 transaction every 2 seconds)
make datagen-start

# Verify data generation
docker logs data-generator --tail 20

# You should see:
# Sent transaction xxx to Kafka
# Sent transaction yyy to Kafka
```

### Step 4: Start Streaming Pipeline

```bash
# Start Spark streaming job (Kafka → Iceberg Bronze)
make streaming-start

# Monitor streaming job
docker logs spark-master --tail 50 --follow

# You should see:
# Batch 0: Written 15 records to Bronze
# Batch 1: Written 15 records to Bronze
```

### Step 5: Access Web UIs

Open these URLs in your browser:

| Service | URL | Purpose |
|---------|-----|---------|
| **Kafka UI** | http://localhost:8080 | Monitor Kafka topics, messages |
| **Spark Master UI** | http://localhost:8888 | Monitor Spark jobs |
| **Trino UI** | http://localhost:8086 | Query execution |
| **MinIO Console** | http://localhost:9001 | Browse data files (minioadmin/minioadmin) |
| **Superset** | http://localhost:8088 | Dashboards (admin/admin) |
| **Grafana** | http://localhost:3000 | Monitoring (admin/admin) |
| **Prometheus** | http://localhost:9090 | Metrics |

---

## Quick Start - Your First 5 Minutes

### Connect to Trino CLI

```bash
# Open Trino CLI
make shell-trino

# Once inside Trino CLI:
trino> SHOW CATALOGS;
```

**Expected Output:**
```
 Catalog
---------
 iceberg
 system
```

### Your First Query

```sql
-- Switch to iceberg catalog
USE iceberg;

-- List schemas (databases)
SHOW SCHEMAS;
```

**Expected Output:**
```
   Schema
------------
 bronze
 gold
 information_schema
 silver
```

### Check Available Tables

```sql
-- Show tables in Bronze layer
SHOW TABLES IN iceberg.bronze;

-- Expected:
--   transactions
```

### Query Bronze Data

```sql
-- Count records in Bronze
SELECT COUNT(*) as total_records
FROM iceberg.bronze.transactions;

-- Expected: ~100-500 records (depending on how long it's been running)
```

### Your First Analytics Query

```sql
-- Top 5 merchants by transaction count
SELECT
    merchant,
    COUNT(*) as transaction_count,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount
FROM iceberg.bronze.transactions
GROUP BY merchant
ORDER BY transaction_count DESC
LIMIT 5;
```

**Expected Output:**
```
  merchant  | transaction_count | total_amount | avg_amount
------------+-------------------+--------------+------------
 Amazon     |               45  |   12,543.21  |   278.74
 Walmart    |               38  |    8,932.45  |   235.06
 Target     |               32  |    7,234.89  |   226.09
 ...
```

**🎉 Congratulations!** You've just queried real-time streaming data in your lakehouse!

---

## Use Case 1: Data Analyst - Exploring Transaction Data

**Persona:** Sarah, Data Analyst
**Goal:** Analyze banking transaction patterns
**Tools:** Trino SQL, SQL Lab
**Time:** 20 minutes

### Scenario

Sarah needs to analyze transaction patterns to understand customer behavior and identify trends.

### Step 1: Basic Exploratory Queries

```sql
-- Connect to Trino
USE iceberg.bronze;

-- 1. Get data overview
SELECT
    COUNT(*) as total_transactions,
    COUNT(DISTINCT account_id) as unique_accounts,
    COUNT(DISTINCT merchant) as unique_merchants,
    MIN(timestamp) as earliest_transaction,
    MAX(timestamp) as latest_transaction,
    ROUND(SUM(amount), 2) as total_volume
FROM transactions;
```

**Expected Output:**
```
total_transactions | unique_accounts | unique_merchants | earliest_transaction      | latest_transaction        | total_volume
-------------------+-----------------+------------------+---------------------------+---------------------------+-------------
              500  |             150 |               15 | 2025-11-19 10:23:45.123  | 2025-11-19 12:45:32.456  | 125,432.50
```

### Step 2: Transaction Type Analysis

```sql
-- 2. Transaction distribution by type
SELECT
    transaction_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount,
    ROUND(MIN(amount), 2) as min_amount,
    ROUND(MAX(amount), 2) as max_amount
FROM transactions
GROUP BY transaction_type
ORDER BY count DESC;
```

**Expected Output:**
```
transaction_type | count | percentage | total_amount | avg_amount | min_amount | max_amount
-----------------+-------+------------+--------------+------------+------------+-----------
PURCHASE        |   225 |      45.00 |    45,678.90 |     203.02 |      10.50 |   4,999.99
WITHDRAWAL      |   100 |      20.00 |    15,234.56 |     152.35 |      20.00 |     999.99
DEPOSIT         |    75 |      15.00 |    35,678.23 |     475.71 |      50.00 |  10,000.00
TRANSFER        |    75 |      15.00 |    25,456.78 |     339.42 |     100.00 |  50,000.00
PAYMENT         |    25 |       5.00 |     3,384.03 |     135.36 |      10.00 |   1,999.99
```

### Step 3: Merchant Analysis

```sql
-- 3. Top 10 merchants by revenue
SELECT
    merchant,
    COUNT(*) as transactions,
    ROUND(SUM(amount), 2) as total_revenue,
    ROUND(AVG(amount), 2) as avg_transaction,
    COUNT(DISTINCT account_id) as unique_customers
FROM transactions
WHERE transaction_type = 'PURCHASE'
GROUP BY merchant
ORDER BY total_revenue DESC
LIMIT 10;
```

### Step 4: Temporal Analysis

```sql
-- 4. Transactions by hour of day
SELECT
    HOUR(CAST(timestamp AS TIMESTAMP)) as hour_of_day,
    COUNT(*) as transactions,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount
FROM transactions
GROUP BY HOUR(CAST(timestamp AS TIMESTAMP))
ORDER BY hour_of_day;
```

### Step 5: Fraud Detection Analysis

```sql
-- 5. Identify potential fraud patterns
SELECT
    account_id,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as flagged_transactions,
    ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) as flagged_amount,
    ROUND(AVG(amount), 2) as avg_transaction,
    MAX(amount) as max_transaction
FROM transactions
GROUP BY account_id
HAVING SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) > 0
ORDER BY flagged_transactions DESC
LIMIT 10;
```

### Step 6: Geographic Analysis

```sql
-- 6. Transaction volume by city
SELECT
    location.city,
    COUNT(*) as transactions,
    ROUND(SUM(amount), 2) as total_amount,
    COUNT(DISTINCT account_id) as unique_customers,
    ROUND(AVG(amount), 2) as avg_transaction
FROM transactions
GROUP BY location.city
ORDER BY total_amount DESC;
```

### Step 7: Cohort Analysis (Advanced)

```sql
-- 7. Daily transaction trends
SELECT
    DATE(CAST(timestamp AS TIMESTAMP)) as transaction_date,
    COUNT(*) as transactions,
    COUNT(DISTINCT account_id) as active_accounts,
    ROUND(SUM(amount), 2) as daily_volume,
    ROUND(AVG(amount), 2) as avg_transaction,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count
FROM transactions
GROUP BY DATE(CAST(timestamp AS TIMESTAMP))
ORDER BY transaction_date;
```

### Step 8: Complex Window Functions

```sql
-- 8. Running totals and moving averages
SELECT
    DATE(CAST(timestamp AS TIMESTAMP)) as date,
    COUNT(*) as daily_transactions,
    ROUND(SUM(amount), 2) as daily_amount,
    ROUND(SUM(SUM(amount)) OVER (ORDER BY DATE(CAST(timestamp AS TIMESTAMP))), 2) as running_total,
    ROUND(AVG(SUM(amount)) OVER (ORDER BY DATE(CAST(timestamp AS TIMESTAMP)) ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) as moving_avg_3day
FROM transactions
GROUP BY DATE(CAST(timestamp AS TIMESTAMP))
ORDER BY date;
```

### 💡 Try This Yourself

**Exercise 1:** Find the busiest merchant per transaction type
```sql
-- Your query here
-- Hint: Use ROW_NUMBER() OVER (PARTITION BY transaction_type ORDER BY COUNT(*) DESC)
```

**Exercise 2:** Calculate the percentage of transactions that are high-value (>$10,000)
```sql
-- Your query here
-- Hint: Use CASE WHEN with aggregation
```

**Exercise 3:** Find accounts with unusual spending patterns (standard deviation)
```sql
-- Your query here
-- Hint: Use STDDEV() function
```

---

## Use Case 2: Business Analyst - Creating Dashboards

**Persona:** Mike, Business Analyst
**Goal:** Create real-time dashboards for executives
**Tools:** Superset, Trino
**Time:** 30 minutes

### Scenario

Mike needs to create executive dashboards showing real-time business metrics.

### Step 1: Access Superset

```bash
# Open browser to http://localhost:8088
# Login: admin / admin
```

### Step 2: Add Trino Database Connection

1. Click **Settings** → **Database Connections**
2. Click **+ Database**
3. Select **Trino** from the list
4. Enter connection details:

```
SQLALCHEMY URI: trino://trino@trino:8086/iceberg
Display Name: Data Lakehouse - Iceberg
```

5. Click **Test Connection** → Should succeed
6. Click **Connect**

### Step 3: Create Your First Chart

**Chart 1: Transaction Volume Over Time**

1. Go to **SQL Lab** → **SQL Editor**
2. Select database: **Data Lakehouse - Iceberg**
3. Run this query:

```sql
SELECT
    DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) as hour,
    COUNT(*) as transactions,
    ROUND(SUM(amount), 2) as volume
FROM bronze.transactions
WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP))
ORDER BY hour;
```

4. Click **Create Chart** → Select **Line Chart**
5. Configure:
   - **X-axis:** hour
   - **Metrics:** transactions, volume
   - **Chart Title:** "Hourly Transaction Volume (Last 24 Hours)"
6. Click **Save**

**Chart 2: Top Merchants (Bar Chart)**

```sql
SELECT
    merchant,
    COUNT(*) as transactions,
    ROUND(SUM(amount), 2) as revenue
FROM bronze.transactions
GROUP BY merchant
ORDER BY revenue DESC
LIMIT 10;
```

**Chart 3: Fraud Detection (Pie Chart)**

```sql
SELECT
    CASE WHEN is_fraud THEN 'Fraudulent' ELSE 'Legitimate' END as status,
    COUNT(*) as count
FROM bronze.transactions
GROUP BY is_fraud;
```

**Chart 4: Geographic Distribution (Table)**

```sql
SELECT
    location.city as city,
    COUNT(*) as transactions,
    ROUND(SUM(amount), 2) as total_amount,
    COUNT(DISTINCT account_id) as customers
FROM bronze.transactions
GROUP BY location.city
ORDER BY total_amount DESC;
```

### Step 4: Create Dashboard

1. Go to **Dashboards** → **+ Dashboard**
2. Name it: "Banking Transaction Metrics - Real-Time"
3. Add the 4 charts you created
4. Arrange them in a 2×2 grid
5. Click **Save**

### Step 5: Add Filters

1. Edit Dashboard
2. Add **Date Filter**:
   - Column: timestamp
   - Default: Last 24 hours
3. Add **Merchant Filter**:
   - Column: merchant
   - Type: Multi-select

### Step 6: Schedule Refresh

1. Dashboard Settings → **Refresh Interval**
2. Set to: **5 minutes** (auto-refresh)
3. Save

### 💡 Try This Yourself

**Exercise 1:** Create a "Fraud Alert" chart showing only high-value fraudulent transactions
```sql
-- Show fraud transactions > $10,000 in last hour
```

**Exercise 2:** Create a heatmap showing transactions by hour and day of week
```sql
-- Hint: Use HOUR() and DAY_OF_WEEK() functions
```

---

## Use Case 3: Data Engineer - Building ETL Pipelines

**Persona:** Alex, Data Engineer
**Goal:** Build Bronze → Silver → Gold ETL pipeline
**Tools:** Spark, Python, SQL
**Time:** 45 minutes

### Scenario

Alex needs to transform raw Bronze data into curated Silver and aggregated Gold tables.

### Step 1: Examine Bronze Data Quality

```bash
# Connect to Trino
make shell-trino
```

```sql
-- Check for data quality issues
SELECT
    COUNT(*) as total_records,
    SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) as null_transaction_id,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amount,
    SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) as invalid_amount,
    SUM(CASE WHEN status NOT IN ('COMPLETED', 'PENDING', 'FAILED') THEN 1 ELSE 0 END) as invalid_status,
    COUNT(DISTINCT transaction_id) as unique_transactions,
    COUNT(*) - COUNT(DISTINCT transaction_id) as duplicate_count
FROM iceberg.bronze.transactions;
```

### Step 2: Create Silver Transformation Job

Create a new file: `spark/jobs/my_silver_transformation.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder \
    .appName("Bronze to Silver ETL") \
    .getOrCreate()

print("=" * 80)
print("Starting Bronze → Silver Transformation")
print("=" * 80)

# Read Bronze data
bronze_df = spark.table("iceberg.bronze.transactions")
initial_count = bronze_df.count()
print(f"Bronze records: {initial_count}")

# Step 1: Deduplication (keep latest by timestamp)
window_spec = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
deduped_df = bronze_df \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

dedup_count = deduped_df.count()
print(f"After deduplication: {dedup_count} (removed {initial_count - dedup_count} duplicates)")

# Step 2: Data Validation
validated_df = deduped_df \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .filter(col("status").isin("COMPLETED", "PENDING", "FAILED"))

valid_count = validated_df.count()
print(f"After validation: {valid_count} (removed {dedup_count - valid_count} invalid records)")

# Step 3: Type Casting & Transformation
silver_df = validated_df \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .withColumn("amount", col("amount").cast("decimal(18,2)")) \
    .withColumn("is_high_value", col("amount") > 10000) \
    .withColumn("risk_score",
        when(col("is_fraud"), 100)
        .when(col("amount") > 50000, 80)
        .when(col("amount") > 10000, 50)
        .otherwise(10)) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .withColumn("processed_date", current_date())

# Step 4: Enrichment (add derived fields)
silver_df = silver_df \
    .withColumn("hour_of_day", hour(col("timestamp"))) \
    .withColumn("day_of_week", dayofweek(col("timestamp"))) \
    .withColumn("is_weekend", dayofweek(col("timestamp")).isin(1, 7)) \
    .withColumn("transaction_category",
        when(col("amount") < 100, "Small")
        .when(col("amount") < 1000, "Medium")
        .when(col("amount") < 10000, "Large")
        .otherwise("Extra Large"))

# Step 5: Write to Silver
print("\nWriting to Silver table...")
silver_df.writeTo("iceberg.silver.transactions") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.parquet.compression-codec", "gzip") \
    .createOrReplace()

final_count = silver_df.count()
print(f"Silver table created with {final_count} records")

# Step 6: Data Quality Report
print("\n" + "=" * 80)
print("Data Quality Report")
print("=" * 80)

quality_report = silver_df.agg(
    count("*").alias("total_records"),
    countDistinct("account_id").alias("unique_accounts"),
    round(sum("amount"), 2).alias("total_amount"),
    round(avg("amount"), 2).alias("avg_amount"),
    sum(when(col("is_high_value"), 1).otherwise(0)).alias("high_value_count"),
    sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count"),
    round(sum(when(col("is_fraud"), col("amount")).otherwise(0)), 2).alias("fraud_amount")
).collect()[0]

print(f"Total Records: {quality_report['total_records']}")
print(f"Unique Accounts: {quality_report['unique_accounts']}")
print(f"Total Amount: ${quality_report['total_amount']:,.2f}")
print(f"Average Amount: ${quality_report['avg_amount']:,.2f}")
print(f"High Value Transactions (>$10K): {quality_report['high_value_count']}")
print(f"Fraud Transactions: {quality_report['fraud_count']}")
print(f"Fraud Amount: ${quality_report['fraud_amount']:,.2f}")

print("\n✅ Silver transformation completed successfully!")

spark.stop()
```

### Step 3: Run Silver Transformation

```bash
# Copy the script to Spark master
docker cp spark/jobs/my_silver_transformation.py spark-master:/opt/spark/jobs/

# Submit the job
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/jobs/my_silver_transformation.py

# Watch the output
# You should see:
# Bronze records: 500
# After deduplication: 498 (removed 2 duplicates)
# After validation: 495 (removed 3 invalid records)
# Silver table created with 495 records
# ✅ Silver transformation completed successfully!
```

### Step 4: Verify Silver Table

```sql
-- Connect to Trino
USE iceberg.silver;

-- Check Silver table
SELECT COUNT(*) FROM transactions;

-- Verify enrichment
SELECT
    transaction_category,
    COUNT(*) as count,
    ROUND(AVG(amount), 2) as avg_amount
FROM transactions
GROUP BY transaction_category
ORDER BY
    CASE transaction_category
        WHEN 'Small' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'Large' THEN 3
        WHEN 'Extra Large' THEN 4
    END;
```

### Step 5: Create Gold Aggregation

Create file: `spark/jobs/my_gold_aggregation.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Silver to Gold Aggregation") \
    .getOrCreate()

print("=" * 80)
print("Creating Gold Tables - Business Metrics")
print("=" * 80)

# Read Silver data
silver_df = spark.table("iceberg.silver.transactions")

# Gold Table 1: Daily Account Summary
print("\n1. Creating daily_account_summary...")
daily_account_summary = silver_df \
    .groupBy("account_id", "date_partition") \
    .agg(
        count("*").alias("transaction_count"),
        sum(when(col("amount") < 0, col("amount")).otherwise(0)).alias("total_debits"),
        sum(when(col("amount") > 0, col("amount")).otherwise(0)).alias("total_credits"),
        sum("amount").alias("net_amount"),
        avg("amount").alias("avg_transaction_amount"),
        max("amount").alias("max_transaction_amount"),
        min("amount").alias("min_transaction_amount"),
        countDistinct("merchant").alias("unique_merchants"),
        sum(when(col("is_fraud"), 1).otherwise(0)).alias("flagged_transactions"),
        sum(when(col("is_high_value"), 1).otherwise(0)).alias("high_value_transactions")
    ) \
    .withColumnRenamed("date_partition", "transaction_date")

daily_account_summary.writeTo("iceberg.gold.daily_account_summary") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

print(f"✅ Created daily_account_summary: {daily_account_summary.count()} records")

# Gold Table 2: Merchant Summary
print("\n2. Creating merchant_summary...")
merchant_summary = silver_df \
    .groupBy("merchant", "date_partition") \
    .agg(
        count("*").alias("transaction_volume"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        max("amount").alias("max_amount"),
        countDistinct("account_id").alias("unique_accounts"),
        sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count")
    ) \
    .withColumnRenamed("date_partition", "transaction_date")

merchant_summary.writeTo("iceberg.gold.merchant_summary") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

print(f"✅ Created merchant_summary: {merchant_summary.count()} records")

# Gold Table 3: Hourly Transaction Trends
print("\n3. Creating hourly_trends...")
hourly_trends = silver_df \
    .withColumn("hour", date_trunc('hour', col("timestamp"))) \
    .groupBy("hour") \
    .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        countDistinct("account_id").alias("active_accounts"),
        sum(when(col("transaction_type") == "PURCHASE", 1).otherwise(0)).alias("purchases"),
        sum(when(col("transaction_type") == "WITHDRAWAL", 1).otherwise(0)).alias("withdrawals"),
        sum(when(col("transaction_type") == "DEPOSIT", 1).otherwise(0)).alias("deposits")
    )

hourly_trends.writeTo("iceberg.gold.hourly_trends") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

print(f"✅ Created hourly_trends: {hourly_trends.count()} records")

print("\n" + "=" * 80)
print("✅ All Gold tables created successfully!")
print("=" * 80)

spark.stop()
```

### Step 6: Run Gold Aggregation

```bash
# Copy script
docker cp spark/jobs/my_gold_aggregation.py spark-master:/opt/spark/jobs/

# Submit job
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/jobs/my_gold_aggregation.py
```

### Step 7: Query Gold Tables

```sql
-- Top performing accounts
SELECT
    account_id,
    SUM(transaction_count) as total_transactions,
    ROUND(SUM(net_amount), 2) as net_amount,
    SUM(flagged_transactions) as fraud_alerts
FROM iceberg.gold.daily_account_summary
GROUP BY account_id
ORDER BY total_transactions DESC
LIMIT 10;

-- Merchant performance
SELECT
    merchant,
    SUM(transaction_volume) as transactions,
    ROUND(SUM(total_amount), 2) as revenue,
    ROUND(AVG(avg_amount), 2) as avg_transaction
FROM iceberg.gold.merchant_summary
GROUP BY merchant
ORDER BY revenue DESC;

-- Hourly activity pattern
SELECT
    HOUR(hour) as hour_of_day,
    ROUND(AVG(transaction_count), 0) as avg_transactions,
    ROUND(AVG(total_amount), 2) as avg_amount
FROM iceberg.gold.hourly_trends
GROUP BY HOUR(hour)
ORDER BY hour_of_day;
```

### 💡 Try This Yourself

**Exercise 1:** Add a data quality check that rejects records with null merchant names

**Exercise 2:** Create a Gold table showing daily fraud statistics by city

**Exercise 3:** Implement slowly changing dimension (SCD) Type 2 for account information

---

## Use Case 4: Real-Time Monitoring - Streaming Data

**Persona:** Emma, Site Reliability Engineer
**Goal:** Monitor real-time data pipeline health
**Tools:** Kafka UI, Spark UI, Grafana
**Time:** 30 minutes

### Scenario

Emma needs to ensure the streaming pipeline is healthy and data is flowing correctly.

### Step 1: Monitor Kafka Topics

```bash
# Open Kafka UI: http://localhost:8080

# Navigate to:
# 1. Topics → banking.transactions.raw
#    - Check message rate
#    - Check partition distribution
#    - Inspect recent messages

# 2. Brokers
#    - Check all 3 brokers are healthy
#    - Check disk usage
#    - Check replication status
```

**CLI Alternative:**
```bash
# Check topic details
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic banking.transactions.raw

# Consume messages (last 10)
docker exec kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:29092 \
  --topic banking.transactions.raw \
  --from-beginning \
  --max-messages 10
```

### Step 2: Monitor Spark Streaming

```bash
# Open Spark UI: http://localhost:8888

# Navigate to:
# 1. Running Applications
#    - Find "KafkaToIcebergStreaming"
#    - Click on it

# 2. Streaming Tab
#    - Check "Input Rate" (records/sec)
#    - Check "Processing Time" (should be < 30s)
#    - Check "Scheduling Delay" (should be < 1s)

# 3. Look for issues:
#    - Red bars = failed batches
#    - High processing time = performance issue
#    - Increasing delay = falling behind
```

**Check Streaming Logs:**
```bash
# View streaming job logs
docker logs spark-master --tail 100 --follow

# Look for:
# ✅ "Batch X: Written Y records to Bronze"
# ❌ Errors, exceptions, retries
```

### Step 3: Monitor Data Ingestion Rate

```sql
-- Connect to Trino
USE iceberg.bronze;

-- Records per minute (last hour)
SELECT
    DATE_TRUNC('minute', CAST(ingestion_timestamp AS TIMESTAMP)) as minute,
    COUNT(*) as records_ingested
FROM transactions
WHERE ingestion_timestamp > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY DATE_TRUNC('minute', CAST(ingestion_timestamp AS TIMESTAMP))
ORDER BY minute DESC
LIMIT 60;

-- Expected: ~30 records per minute (1 every 2 seconds × 30 = 15-30)
```

### Step 4: Check Data Freshness (Lag)

```sql
-- How old is the newest data?
SELECT
    MAX(CAST(timestamp AS TIMESTAMP)) as latest_transaction_time,
    MAX(CAST(ingestion_timestamp AS TIMESTAMP)) as latest_ingestion_time,
    CURRENT_TIMESTAMP as current_time,
    EXTRACT(SECOND FROM (CURRENT_TIMESTAMP - MAX(CAST(ingestion_timestamp AS TIMESTAMP)))) as lag_seconds
FROM transactions;

-- lag_seconds should be < 60 seconds
-- If > 120 seconds, streaming job may be stopped or slow
```

### Step 5: Monitor with Grafana

```bash
# Open Grafana: http://localhost:3000
# Login: admin/admin

# Create dashboard with these queries:
```

**Panel 1: Kafka Message Rate**
```promql
# Prometheus query
rate(kafka_server_brokertopicmetrics_messagesinpersec_count[1m])
```

**Panel 2: Spark Batch Duration**
```promql
# Custom metric (if configured)
spark_streaming_batch_duration_ms
```

**Panel 3: Data Freshness**
```sql
-- Add Trino data source to Grafana
-- Query:
SELECT
    EXTRACT(SECOND FROM (CURRENT_TIMESTAMP - MAX(CAST(ingestion_timestamp AS TIMESTAMP)))) as lag_seconds
FROM iceberg.bronze.transactions
```

### Step 6: Set Up Alerts

Create alert in Grafana:

**Alert 1: Streaming Job Down**
```
Condition: lag_seconds > 300
Alert message: "Streaming job may be down. No data ingested in 5 minutes."
Notification: Email/Slack
```

**Alert 2: High Processing Time**
```
Condition: spark_streaming_batch_duration_ms > 30000
Alert message: "Spark batch taking > 30s. Risk of falling behind."
Notification: Email/Slack
```

### Step 7: Troubleshoot Issues

**Issue: No new data in Bronze**

```bash
# 1. Check data generator
docker logs data-generator --tail 20

# Should see: "Sent transaction xxx to Kafka"
# If not, restart: make datagen-restart

# 2. Check Kafka topic has messages
docker exec kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:29092 \
  --topic banking.transactions.raw \
  --max-messages 5

# 3. Check Spark streaming job
docker logs spark-master --tail 50

# Should see batch logs
# If not running: make streaming-restart
```

### 💡 Try This Yourself

**Exercise 1:** Create a Grafana dashboard showing end-to-end latency

**Exercise 2:** Set up an alert for when fraud rate exceeds 5%

**Exercise 3:** Monitor MinIO storage growth over time

---

## Use Case 5: Data Quality Engineer - Validation & Testing

**Persona:** Lisa, Data Quality Engineer
**Goal:** Ensure data quality across all layers
**Tools:** SQL, Great Expectations (concept)
**Time:** 30 minutes

### Scenario

Lisa needs to validate data quality and catch issues early.

### Step 1: Bronze Layer Validation

```sql
-- Data Quality Report: Bronze Layer
WITH quality_checks AS (
  SELECT
    COUNT(*) as total_records,

    -- Completeness checks
    SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) as null_transaction_id,
    SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamp,
    SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) as null_account_id,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amount,
    SUM(CASE WHEN merchant IS NULL THEN 1 ELSE 0 END) as null_merchant,

    -- Validity checks
    SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) as invalid_amount,
    SUM(CASE WHEN status NOT IN ('COMPLETED', 'PENDING', 'FAILED') THEN 1 ELSE 0 END) as invalid_status,
    SUM(CASE WHEN transaction_type NOT IN ('PURCHASE', 'WITHDRAWAL', 'DEPOSIT', 'TRANSFER', 'PAYMENT') THEN 1 ELSE 0 END) as invalid_type,

    -- Uniqueness checks
    COUNT(DISTINCT transaction_id) as unique_transaction_ids,

    -- Consistency checks
    SUM(CASE WHEN amount > 100000 THEN 1 ELSE 0 END) as outlier_amounts,
    SUM(CASE WHEN CAST(timestamp AS TIMESTAMP) > CURRENT_TIMESTAMP THEN 1 ELSE 0 END) as future_timestamps

  FROM iceberg.bronze.transactions
)
SELECT
  total_records,

  -- Completeness %
  ROUND(100.0 * (total_records - null_transaction_id) / total_records, 2) as transaction_id_completeness,
  ROUND(100.0 * (total_records - null_amount) / total_records, 2) as amount_completeness,
  ROUND(100.0 * (total_records - null_merchant) / total_records, 2) as merchant_completeness,

  -- Validity %
  ROUND(100.0 * (total_records - invalid_amount) / total_records, 2) as amount_validity,
  ROUND(100.0 * (total_records - invalid_status) / total_records, 2) as status_validity,

  -- Uniqueness %
  ROUND(100.0 * unique_transaction_ids / total_records, 2) as uniqueness,

  -- Issue counts
  invalid_amount,
  invalid_status,
  invalid_type,
  outlier_amounts,
  future_timestamps,
  total_records - unique_transaction_ids as duplicate_count

FROM quality_checks;
```

### Step 2: Data Profiling

```sql
-- Statistical Profile
SELECT
  'amount' as column_name,
  COUNT(*) as count,
  COUNT(DISTINCT amount) as distinct_count,
  ROUND(MIN(amount), 2) as min_value,
  ROUND(MAX(amount), 2) as max_value,
  ROUND(AVG(amount), 2) as mean,
  ROUND(APPROX_PERCENTILE(amount, 0.5), 2) as median,
  ROUND(STDDEV(amount), 2) as std_dev,
  ROUND(APPROX_PERCENTILE(amount, 0.25), 2) as q1,
  ROUND(APPROX_PERCENTILE(amount, 0.75), 2) as q3
FROM iceberg.bronze.transactions;
```

### Step 3: Anomaly Detection

```sql
-- Detect anomalies using statistical methods
WITH stats AS (
  SELECT
    AVG(amount) as mean_amount,
    STDDEV(amount) as std_amount
  FROM iceberg.bronze.transactions
),
z_scores AS (
  SELECT
    t.*,
    (t.amount - s.mean_amount) / s.std_amount as z_score
  FROM iceberg.bronze.transactions t
  CROSS JOIN stats s
)
SELECT
  transaction_id,
  account_id,
  amount,
  merchant,
  ROUND(z_score, 2) as z_score,
  CASE
    WHEN ABS(z_score) > 3 THEN 'Extreme Outlier'
    WHEN ABS(z_score) > 2 THEN 'Outlier'
    ELSE 'Normal'
  END as classification
FROM z_scores
WHERE ABS(z_score) > 2
ORDER BY ABS(z_score) DESC;
```

### Step 4: Referential Integrity Checks

```sql
-- Check if all accounts in transactions exist in accounts table (hypothetical)
-- Assuming we have an accounts reference table

-- For this demo, check consistency within transactions
SELECT
  account_id,
  COUNT(DISTINCT customer_email) as email_count,
  COUNT(DISTINCT customer_name) as name_count
FROM iceberg.bronze.transactions
GROUP BY account_id
HAVING COUNT(DISTINCT customer_email) > 1
    OR COUNT(DISTINCT customer_name) > 1;

-- If results returned: Same account has different customer info (inconsistency)
```

### Step 5: Temporal Data Quality

```sql
-- Check for temporal anomalies
SELECT
  DATE(CAST(timestamp AS TIMESTAMP)) as date,
  COUNT(*) as records,
  ROUND(AVG(amount), 2) as avg_amount,

  -- Day-over-day comparison
  LAG(COUNT(*)) OVER (ORDER BY DATE(CAST(timestamp AS TIMESTAMP))) as prev_day_records,

  -- % change
  ROUND(100.0 * (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE(CAST(timestamp AS TIMESTAMP)))) /
        NULLIF(LAG(COUNT(*)) OVER (ORDER BY DATE(CAST(timestamp AS TIMESTAMP))), 0), 2) as pct_change

FROM iceberg.bronze.transactions
GROUP BY DATE(CAST(timestamp AS TIMESTAMP))
ORDER BY date;

-- Alert if pct_change > 50% or < -50% (unusual spike or drop)
```

### Step 6: Create Data Quality Dashboard

```sql
-- Create a Gold table for data quality metrics
CREATE TABLE IF NOT EXISTS iceberg.gold.data_quality_metrics (
  check_timestamp TIMESTAMP,
  layer VARCHAR,
  metric_name VARCHAR,
  metric_value DOUBLE,
  status VARCHAR,
  details VARCHAR
);

-- Insert quality metrics
INSERT INTO iceberg.gold.data_quality_metrics
SELECT
  CURRENT_TIMESTAMP as check_timestamp,
  'bronze' as layer,
  'completeness' as metric_name,
  100.0 * COUNT(*) / COUNT(*) as metric_value,  -- Placeholder
  CASE
    WHEN 100.0 * COUNT(*) / COUNT(*) > 99 THEN 'PASS'
    ELSE 'FAIL'
  END as status,
  'Transaction ID completeness check' as details
FROM iceberg.bronze.transactions;
```

### 💡 Try This Yourself

**Exercise 1:** Create a data quality score (0-100) for the Bronze layer

**Exercise 2:** Implement a "freshness" check (alert if no data in last 5 min)

**Exercise 3:** Build a quarantine table for failed records

---

## Use Case 6: Platform Engineer - Time Travel & Schema Evolution

**Persona:** Tom, Platform Engineer
**Goal:** Leverage Iceberg advanced features
**Tools:** Trino, Spark, Iceberg API
**Time:** 30 minutes

### Scenario

Tom needs to use Iceberg's time travel to recover from an error and evolve table schemas.

### Step 1: View Iceberg Snapshots

```sql
-- Connect to Trino
USE iceberg.bronze;

-- View table history (snapshots)
SELECT
  made_current_at,
  snapshot_id,
  parent_id,
  operation,
  summary['total-records'] as total_records,
  summary['total-data-files'] as total_files
FROM "transactions$snapshots"
ORDER BY made_current_at DESC
LIMIT 10;
```

**Expected Output:**
```
made_current_at          | snapshot_id        | operation | total_records | total_files
-------------------------+--------------------+-----------+---------------+-------------
2025-11-19 14:30:00.123 | 789456123789456123 | append    |           520 |           5
2025-11-19 14:29:30.456 | 789456123789456122 | append    |           500 |           5
2025-11-19 14:29:00.789 | 789456123789456121 | append    |           480 |           5
...
```

### Step 2: Time Travel Query (Query Historical Data)

```sql
-- Query current state
SELECT COUNT(*) as current_count
FROM iceberg.bronze.transactions;

-- Query as of 10 minutes ago
SELECT COUNT(*) as count_10min_ago
FROM iceberg.bronze.transactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2025-11-19 14:20:00';

-- Query specific snapshot
SELECT COUNT(*) as count_snapshot
FROM iceberg.bronze.transactions
FOR SYSTEM_VERSION AS OF 789456123789456121;

-- Compare current vs. previous snapshot
SELECT
  'current' as version,
  COUNT(*) as records,
  ROUND(SUM(amount), 2) as total_amount
FROM iceberg.bronze.transactions

UNION ALL

SELECT
  'previous' as version,
  COUNT(*) as records,
  ROUND(SUM(amount), 2) as total_amount
FROM iceberg.bronze.transactions
FOR SYSTEM_VERSION AS OF 789456123789456121;
```

### Step 3: Scenario - Recover from Bad Data

```sql
-- Scenario: Accidentally wrote bad data in last batch
-- Let's say snapshot 789456123789456123 has corrupt data

-- Step 1: Verify issue
SELECT *
FROM iceberg.bronze.transactions
FOR SYSTEM_VERSION AS OF 789456123789456123
WHERE amount < 0  -- Invalid data
LIMIT 10;

-- Step 2: Rollback to previous good snapshot
-- (This requires Spark, not Trino)
```

**Rollback Script (Spark):**

```python
# In Spark SQL
spark.sql("""
  CALL iceberg.system.rollback_to_snapshot(
    'iceberg.bronze.transactions',
    789456123789456122
  )
""")

# Or rollback to timestamp
spark.sql("""
  CALL iceberg.system.rollback_to_timestamp(
    'iceberg.bronze.transactions',
    TIMESTAMP '2025-11-19 14:29:00'
  )
""")
```

**Execute Rollback:**

```bash
# Create rollback script
cat > rollback.py << 'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rollback").getOrCreate()

# Get current snapshot
current = spark.sql("SELECT snapshot_id FROM iceberg.bronze.\"transactions$snapshots\" ORDER BY made_current_at DESC LIMIT 1").collect()[0][0]
print(f"Current snapshot: {current}")

# Get previous snapshot
previous = spark.sql("SELECT snapshot_id FROM iceberg.bronze.\"transactions$snapshots\" ORDER BY made_current_at DESC LIMIT 1 OFFSET 1").collect()[0][0]
print(f"Previous snapshot: {previous}")

# Rollback
spark.sql(f"""
  CALL iceberg.system.rollback_to_snapshot(
    'iceberg.bronze.transactions',
    {previous}
  )
""")

print("✅ Rollback successful!")
spark.stop()
EOF

# Execute
docker cp rollback.py spark-master:/tmp/
docker exec spark-master spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  /tmp/rollback.py
```

### Step 4: Schema Evolution

**Scenario:** Add new columns to track device fingerprint and IP address

```python
# Create schema evolution script
cat > add_columns.py << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Schema Evolution").getOrCreate()

# Read current table
df = spark.table("iceberg.bronze.transactions")
print(f"Current schema has {len(df.columns)} columns")

# Add new columns
df_evolved = df \
    .withColumn("device_fingerprint", lit(None).cast(StringType())) \
    .withColumn("ip_address", lit(None).cast(StringType())) \
    .withColumn("user_agent", lit(None).cast(StringType()))

# Write back (schema evolution happens automatically)
df_evolved.limit(0).writeTo("iceberg.bronze.transactions") \
    .using("iceberg") \
    .append()

print(f"✅ Schema evolved! New schema has {len(df_evolved.columns)} columns")

# Verify
new_df = spark.table("iceberg.bronze.transactions")
print("\nNew schema:")
new_df.printSchema()

spark.stop()
EOF

# Execute
docker cp add_columns.py spark-master:/tmp/
docker exec spark-master spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  /tmp/add_columns.py
```

### Step 5: Query with New Schema

```sql
-- Old data: new columns are NULL
-- New data: new columns populated

SELECT
  transaction_id,
  amount,
  device_fingerprint,  -- New column
  ip_address,          -- New column
  user_agent           -- New column
FROM iceberg.bronze.transactions
LIMIT 10;

-- Backwards compatible: Old queries still work!
SELECT transaction_id, amount
FROM iceberg.bronze.transactions
LIMIT 10;
```

### Step 6: Snapshot Expiration (Cleanup)

```python
# Expire old snapshots (older than 7 days)
spark.sql("""
  CALL iceberg.system.expire_snapshots(
    table => 'iceberg.bronze.transactions',
    older_than => TIMESTAMP '2025-11-12 00:00:00',
    retain_last => 10
  )
""")

# Remove orphan files (failed writes)
spark.sql("""
  CALL iceberg.system.remove_orphan_files(
    table => 'iceberg.bronze.transactions'
  )
""")

# Rewrite small files (compaction)
spark.sql("""
  CALL iceberg.system.rewrite_data_files(
    table => 'iceberg.bronze.transactions',
    options => map('target-file-size-mb', '512')
  )
""")
```

### 💡 Try This Yourself

**Exercise 1:** Find the snapshot where a specific transaction first appeared

**Exercise 2:** Rename a column using Iceberg schema evolution

**Exercise 3:** Create a view that unions current and historical data

---

## Use Case 7: Performance Tuning - Optimization

**Persona:** Rachel, Performance Engineer
**Goal:** Optimize query performance
**Tools:** Trino EXPLAIN, Iceberg metadata
**Time:** 30 minutes

### Scenario

Rachel notices slow queries and needs to optimize.

### Step 1: Identify Slow Queries

```sql
-- Find long-running queries in Trino
SELECT
  query_id,
  query,
  state,
  queued_time_ms,
  execution_time_ms,
  total_cpu_time_ms,
  total_memory_bytes / 1024 / 1024 as memory_mb
FROM system.runtime.queries
WHERE execution_time_ms > 10000  -- > 10 seconds
ORDER BY execution_time_ms DESC
LIMIT 10;
```

### Step 2: Analyze Query Plan

```sql
-- Get execution plan for slow query
EXPLAIN (TYPE DISTRIBUTED)
SELECT
  merchant,
  COUNT(*) as txn_count,
  SUM(amount) as total_amount
FROM iceberg.bronze.transactions
WHERE date_partition >= '2025-11-01'
  AND amount > 1000
GROUP BY merchant;

-- Look for:
-- ❌ Full table scan (no partition pruning)
-- ❌ Large shuffle (expensive GROUP BY)
-- ❌ Remote exchange (data moving between workers)
```

### Step 3: Optimize with Partitioning

```sql
-- BAD: Full table scan (no partition filter)
SELECT COUNT(*)
FROM iceberg.bronze.transactions
WHERE CAST(timestamp AS DATE) >= DATE '2025-11-01';

-- GOOD: Partition pruning (uses date_partition)
SELECT COUNT(*)
FROM iceberg.bronze.transactions
WHERE date_partition >= '2025-11-01';

-- Verify partition pruning
EXPLAIN
SELECT COUNT(*)
FROM iceberg.bronze.transactions
WHERE date_partition = '2025-11-19';

-- Should see: "Partition filter: date_partition = '2025-11-19'"
```

### Step 4: Analyze Table Statistics

```sql
-- View file-level statistics
SELECT
  file_path,
  file_size_in_bytes / 1024 / 1024 as file_size_mb,
  record_count,
  partition
FROM iceberg.bronze."transactions$files"
ORDER BY file_size_in_bytes DESC
LIMIT 20;

-- Check for small file problem
SELECT
  CASE
    WHEN file_size_in_bytes < 1024 * 1024 THEN '< 1 MB'
    WHEN file_size_in_bytes < 10 * 1024 * 1024 THEN '1-10 MB'
    WHEN file_size_in_bytes < 100 * 1024 * 1024 THEN '10-100 MB'
    ELSE '> 100 MB'
  END as file_size_range,
  COUNT(*) as file_count,
  SUM(record_count) as total_records
FROM iceberg.bronze."transactions$files"
GROUP BY 1
ORDER BY 1;

-- Too many small files = slow queries
-- Solution: Compaction (rewrite_data_files)
```

### Step 5: Compaction (Fix Small Files)

```python
# Compact small files into larger ones
cat > compact.py << 'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Compaction").getOrCreate()

# Get file statistics before
before = spark.sql("""
  SELECT
    COUNT(*) as file_count,
    SUM(file_size_in_bytes) / 1024 / 1024 as total_size_mb,
    AVG(file_size_in_bytes) / 1024 / 1024 as avg_size_mb
  FROM iceberg.bronze."transactions$files"
""").collect()[0]

print("Before compaction:")
print(f"  Files: {before['file_count']}")
print(f"  Total size: {before['total_size_mb']:.2f} MB")
print(f"  Avg file size: {before['avg_size_mb']:.2f} MB")

# Compact files
print("\nCompacting...")
spark.sql("""
  CALL iceberg.system.rewrite_data_files(
    table => 'iceberg.bronze.transactions',
    options => map(
      'target-file-size-mb', '128',
      'min-file-size-mb', '10'
    )
  )
""")

# Get file statistics after
after = spark.sql("""
  SELECT
    COUNT(*) as file_count,
    SUM(file_size_in_bytes) / 1024 / 1024 as total_size_mb,
    AVG(file_size_in_bytes) / 1024 / 1024 as avg_size_mb
  FROM iceberg.bronze."transactions$files"
""").collect()[0]

print("\nAfter compaction:")
print(f"  Files: {after['file_count']}")
print(f"  Total size: {after['total_size_mb']:.2f} MB")
print(f"  Avg file size: {after['avg_size_mb']:.2f} MB")

print(f"\n✅ Reduced from {before['file_count']} to {after['file_count']} files!")

spark.stop()
EOF

docker cp compact.py spark-master:/tmp/
docker exec spark-master spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  /tmp/compact.py
```

### Step 6: Optimize Gold Tables for BI

```sql
-- Create a materialized view (pre-aggregated) for common dashboard query
CREATE TABLE iceberg.gold.merchant_performance_mv AS
SELECT
  merchant,
  DATE_TRUNC('day', CAST(timestamp AS TIMESTAMP)) as day,
  COUNT(*) as transaction_count,
  ROUND(SUM(amount), 2) as total_amount,
  ROUND(AVG(amount), 2) as avg_amount,
  COUNT(DISTINCT account_id) as unique_customers
FROM iceberg.bronze.transactions
GROUP BY merchant, DATE_TRUNC('day', CAST(timestamp AS TIMESTAMP));

-- Query materialized view (100x faster than raw data)
SELECT * FROM iceberg.gold.merchant_performance_mv
WHERE day = CURRENT_DATE
ORDER BY total_amount DESC;
```

### Step 7: Benchmark Queries

```sql
-- Benchmark query performance
-- Run this query 5 times, note execution time

-- Test 1: Full table scan (slowest)
SELECT COUNT(*), SUM(amount)
FROM iceberg.bronze.transactions;

-- Test 2: With partition filter (faster)
SELECT COUNT(*), SUM(amount)
FROM iceberg.bronze.transactions
WHERE date_partition = CURRENT_DATE;

-- Test 3: From Gold table (fastest)
SELECT SUM(transaction_count), SUM(total_amount)
FROM iceberg.gold.merchant_performance_mv
WHERE day = CURRENT_DATE;

-- Compare execution times in Trino UI
```

### 💡 Try This Yourself

**Exercise 1:** Create sorted tables (Z-ordering) for better predicate pushdown

**Exercise 2:** Measure query performance before/after compaction

**Exercise 3:** Create partition pruning strategy for multi-month queries

---

## Troubleshooting & Experimentation

### Experiment 1: Simulate Data Generator Failure

```bash
# Stop data generator
make datagen-stop

# Wait 2 minutes

# Query freshness
USE iceberg.bronze;
SELECT
  MAX(CAST(ingestion_timestamp AS TIMESTAMP)) as last_ingestion,
  CURRENT_TIMESTAMP as now,
  EXTRACT(SECOND FROM (CURRENT_TIMESTAMP - MAX(CAST(ingestion_timestamp AS TIMESTAMP)))) as lag_seconds
FROM transactions;

# lag_seconds should be > 120

# Restart generator
make datagen-start

# Verify recovery
# Run freshness query again
```

### Experiment 2: Force Spark Streaming Failure

```bash
# Kill Spark worker
docker kill spark-worker-1

# Check Spark UI - should show failed executor
# Streaming job should continue (fault tolerance)

# Restart worker
docker start spark-worker-1

# Verify: Streaming continues without data loss
```

### Experiment 3: Fill Up Kafka

```bash
# Generate data much faster
docker exec data-generator sh -c '
  sed -i "s/GENERATION_INTERVAL_MS=2000/GENERATION_INTERVAL_MS=100/g" /app/generator.py
'

# Restart generator
make datagen-restart

# Now generating 10 records/sec instead of 0.5

# Monitor:
# 1. Kafka disk usage (http://localhost:8080)
# 2. Spark processing lag (http://localhost:8888)
# 3. MinIO storage growth (http://localhost:9001)

# Observe: Kafka buffers data, Spark falls behind

# Reset
docker exec data-generator sh -c '
  sed -i "s/GENERATION_INTERVAL_MS=100/GENERATION_INTERVAL_MS=2000/g" /app/generator.py
'
make datagen-restart
```

### Experiment 4: Break and Fix Schema

```sql
-- Try to insert incompatible data
-- This will fail (good! schema enforcement)

INSERT INTO iceberg.bronze.transactions (transaction_id, amount)
VALUES ('test', 'not_a_number');  -- Error: type mismatch

-- Schema evolution: Add column
ALTER TABLE iceberg.bronze.transactions
ADD COLUMN new_field VARCHAR;

-- Query still works
SELECT * FROM iceberg.bronze.transactions LIMIT 10;
```

### Experiment 5: Performance Test

```bash
# Increase data volume
# Let generator run overnight (86,400 seconds / 2 = 43,200 records)

# Next day, run queries:
SELECT COUNT(*) FROM iceberg.bronze.transactions;
-- Should return 40K+

# Benchmark query performance at scale
EXPLAIN ANALYZE
SELECT merchant, COUNT(*), SUM(amount)
FROM iceberg.bronze.transactions
WHERE date_partition >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY merchant;
```

---

## Next Steps

### Level Up Your Skills

1. **Build a Custom Use Case:**
   - Define your own business problem
   - Create Bronze → Silver → Gold pipeline
   - Build Superset dashboard

2. **Integrate External Data:**
   - Add a new Kafka topic
   - Ingest CSV files into Bronze
   - Join with existing data in Silver

3. **Implement Advanced Features:**
   - Change Data Capture (CDC) from PostgreSQL
   - Slowly Changing Dimensions (SCD Type 2)
   - Real-time alerts with Kafka Streams

4. **Scale the Platform:**
   - Add more Kafka partitions
   - Increase Spark workers
   - Optimize for 1M+ records

### Resources

- **Iceberg Documentation:** https://iceberg.apache.org/
- **Trino Documentation:** https://trino.io/docs/current/
- **Spark Streaming Guide:** https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- **Superset Documentation:** https://superset.apache.org/docs/intro

---

**🎉 Congratulations!** You've completed the hands-on tutorial. You now have practical experience with:

✅ Querying streaming data with Trino
✅ Building ETL pipelines with Spark
✅ Creating dashboards with Superset
✅ Monitoring real-time pipelines
✅ Ensuring data quality
✅ Using Iceberg time travel and schema evolution
✅ Performance tuning and optimization

Keep experimenting and building! 🚀
