# Batch Processing Pipeline Guide

Complete guide for running batch data transformations using Spark and the Medallion Architecture.

## Table of Contents

1. [Overview](#overview)
2. [Architecture: Medallion Pattern](#architecture-medallion-pattern)
3. [Pipeline Components](#pipeline-components)
4. [Running the Pipeline](#running-the-pipeline)
5. [Querying Processed Data](#querying-processed-data)
6. [Scheduling & Automation](#scheduling--automation)
7. [Customization](#customization)
8. [Monitoring & Troubleshooting](#monitoring--troubleshooting)

---

## Overview

The **batch processing pipeline** transforms raw data from the Bronze layer into cleaned, validated, and aggregated data in Silver and Gold layers.

### What It Does

```
┌──────────────────────────────────────────────────────────┐
│ BRONZE LAYER (Raw Data)                                  │
│ ┌──────────────────────────────────────────────────────┐│
│ │ • Raw transactions from Kafka                         ││
│ │ • No validation applied                               ││
│ │ • May contain duplicates                              ││
│ │ • May contain invalid data                            ││
│ └──────────────────────────────────────────────────────┘│
└────────────────────┬─────────────────────────────────────┘
                     │ Spark Batch Job
                     │ • Remove duplicates
                     │ • Validate data quality
                     │ • Flag suspicious transactions
                     ▼
┌──────────────────────────────────────────────────────────┐
│ SILVER LAYER (Cleaned & Validated)                       │
│ ┌──────────────────────────────────────────────────────┐│
│ │ • Deduplicated transactions                           ││
│ │ • Validated amounts (> 0)                             ││
│ │ • Validated status values                             ││
│ │ • High-value transactions flagged                     ││
│ │ • Validation status added                             ││
│ └──────────────────────────────────────────────────────┘│
└────────────────────┬─────────────────────────────────────┘
                     │ Spark Batch Job
                     │ • Group by account & date
                     │ • Calculate aggregates
                     │ • Generate business metrics
                     ▼
┌──────────────────────────────────────────────────────────┐
│ GOLD LAYER (Business Aggregates)                         │
│ ┌──────────────────────────────────────────────────────┐│
│ │ • Daily account summaries                             ││
│ │ • Total debits/credits                                ││
│ │ • Net amounts                                         ││
│ │ • Transaction statistics                              ││
│ │ • Merchant analytics                                  ││
│ └──────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────┘
```

### Key Benefits

✅ **Data Quality**: Automatic validation and cleaning
✅ **Deduplication**: Removes duplicate transactions
✅ **Business Metrics**: Pre-calculated aggregates
✅ **Performance**: Optimized for analytics queries
✅ **Separation of Concerns**: Raw vs. curated data
✅ **Auditability**: Track processing timestamps

---

## Architecture: Medallion Pattern

The pipeline implements the **Medallion Architecture** (also called Multi-Hop Architecture):

### Layer 1: Bronze (Raw)

**Purpose**: Store raw, unmodified data exactly as received

**Characteristics**:
- No transformations applied
- Preserves original data structure
- Append-only (immutable)
- May contain errors, duplicates
- Full audit trail

**Table**: `iceberg.bronze.transactions`

**Created by**: Spark Streaming job (from Kafka)

### Layer 2: Silver (Cleaned)

**Purpose**: Cleaned, validated, and enriched data ready for analytics

**Transformations Applied**:
- ✅ **Deduplication**: Remove duplicate transaction_ids
- ✅ **Validation**: Filter invalid amounts (must be > 0)
- ✅ **Status Validation**: Only COMPLETED, PENDING, FAILED
- ✅ **Flagging**: Mark high-value transactions (>$10,000)
- ✅ **Enrichment**: Add validation_status column
- ✅ **Timestamping**: Add processing_timestamp

**Table**: `iceberg.silver.transactions`

**Schema**:
```sql
CREATE TABLE iceberg.silver.transactions (
    transaction_id STRING,
    account_id STRING,
    transaction_type STRING,
    amount DECIMAL(18,2),
    currency STRING,
    merchant STRING,
    merchant_category STRING,
    transaction_timestamp TIMESTAMP,
    status STRING,
    is_flagged BOOLEAN,              -- NEW: High-value flag
    validation_status STRING,         -- NEW: VALID/INVALID
    processing_timestamp TIMESTAMP,   -- NEW: When processed
    date_partition DATE
)
PARTITIONED BY (days(date_partition));
```

### Layer 3: Gold (Aggregated)

**Purpose**: Business-level aggregates optimized for reporting and BI tools

**Aggregations Provided**:

**1. Daily Account Summary** (`iceberg.gold.daily_account_summary`)
- Total transactions per account per day
- Total debits and credits
- Net amount (credits - debits)
- Average, min, max transaction amounts
- Count of distinct merchants
- Count of flagged transactions

**Schema**:
```sql
CREATE TABLE iceberg.gold.daily_account_summary (
    account_id STRING,
    transaction_date DATE,
    total_transactions BIGINT,
    total_debits DECIMAL(18,2),
    total_credits DECIMAL(18,2),
    net_amount DECIMAL(18,2),
    avg_transaction_amount DECIMAL(18,2),
    max_transaction_amount DECIMAL(18,2),
    min_transaction_amount DECIMAL(18,2),
    distinct_merchants BIGINT,
    flagged_transactions BIGINT,
    processing_timestamp TIMESTAMP
)
PARTITIONED BY (days(transaction_date));
```

**2. Merchant Summary** (`iceberg.gold.merchant_summary`)
- Transaction counts by merchant
- Total revenue by merchant
- Average transaction amount
- Unique customer counts

**Schema**:
```sql
CREATE TABLE iceberg.gold.merchant_summary (
    merchant STRING,
    merchant_category STRING,
    transaction_date DATE,
    total_transactions BIGINT,
    total_amount DECIMAL(18,2),
    avg_amount DECIMAL(18,2),
    unique_accounts BIGINT,
    processing_timestamp TIMESTAMP
)
PARTITIONED BY (days(transaction_date));
```

---

## Pipeline Components

### Spark Job: `batch_aggregations.py`

**Location**: `spark/jobs/batch_aggregations.py`

**Main Functions**:

1. **`bronze_to_silver()`**
   - Reads from Bronze layer
   - Applies data quality rules
   - Removes duplicates
   - Flags high-value transactions
   - Writes to Silver layer

2. **`silver_to_gold()`**
   - Reads from Silver layer
   - Groups by account and date
   - Calculates aggregates
   - Writes to Gold layer

3. **`generate_merchant_analytics()`**
   - Creates merchant-level summaries
   - Analyzes revenue by merchant
   - Counts unique customers

### Processing Logic

**Data Quality Rules (Bronze → Silver)**:

```python
# Remove duplicates by transaction_id
.dropDuplicates(["transaction_id"])

# Only positive amounts
.filter(col("amount") > 0)

# Valid status values only
.filter(col("status").isin(["COMPLETED", "PENDING", "FAILED"]))

# Flag high-value transactions
.withColumn("is_flagged", when(col("amount") > 10000, True).otherwise(False))

# Add validation status
.withColumn("validation_status", when(col("amount") > 0, "VALID").otherwise("INVALID"))

# Add processing timestamp
.withColumn("processing_timestamp", current_timestamp())
```

**Aggregation Logic (Silver → Gold)**:

```python
# Group by account and date
.groupBy("account_id", "date_partition")

# Calculate metrics
.agg(
    count("*").alias("total_transactions"),
    sum(when(col("transaction_type") == "DEBIT", col("amount"))).alias("total_debits"),
    sum(when(col("transaction_type") == "CREDIT", col("amount"))).alias("total_credits"),
    avg("amount").alias("avg_transaction_amount"),
    max("amount").alias("max_transaction_amount"),
    min("amount").alias("min_transaction_amount"),
    countDistinct("merchant").alias("distinct_merchants"),
    sum(when(col("is_flagged"), 1)).alias("flagged_transactions")
)
```

---

## Running the Pipeline

### Prerequisites

1. **Bronze layer must have data**:
   ```bash
   # Ensure streaming pipeline is running
   bash guides/data-ingestion-processing/scripts/start-pipeline.sh

   # Wait for data to accumulate (1-2 minutes)
   ```

2. **Verify Bronze data**:
   ```bash
   make shell-trino
   ```
   ```sql
   SELECT COUNT(*) FROM iceberg.bronze.transactions;
   -- Should show > 0 records
   ```

### Method 1: Use the Helper Script (Easiest)

```bash
# Run the complete batch pipeline
bash guides/data-ingestion-processing/scripts/run-batch-pipeline.sh
```

**What it does**:
- ✅ Checks Bronze layer has data
- ✅ Submits Spark batch job
- ✅ Processes Bronze → Silver → Gold
- ✅ Verifies results
- ✅ Shows record counts

**Expected output**:
```
==========================================
Spark Batch Aggregations Pipeline
==========================================

Checking Bronze layer data...
✓ Found 450 records in Bronze layer

Submitting Spark batch aggregations job...
This will process:
  1. Bronze → Silver (data cleaning & validation)
  2. Silver → Gold (business aggregations)
  3. Additional merchant analytics

[Spark job output...]

==========================================
Batch Processing Complete!
==========================================

Checking results...
✓ Silver layer: 442 records
✓ Gold layer (daily_account_summary): 85 records
✓ Merchant summary: 12 records
```

### Method 2: Manual Spark Submit

```bash
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
```

### Method 3: Interactive Spark Shell

For testing and development:

```bash
docker exec -it spark-master pyspark \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://lakehouse/
```

Then run transformations manually:
```python
# Read Bronze
bronze_df = spark.table("local.bronze.transactions")

# Apply transformations
silver_df = bronze_df \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("amount") > 0) \
    .show()
```

---

## Querying Processed Data

### Connect to Trino

```bash
make shell-trino
```

### Silver Layer Queries

**View cleaned transactions**:
```sql
SELECT * FROM iceberg.silver.transactions
ORDER BY transaction_timestamp DESC
LIMIT 10;
```

**Count valid vs. flagged**:
```sql
SELECT
    validation_status,
    is_flagged,
    COUNT(*) as count,
    SUM(amount) as total_amount
FROM iceberg.silver.transactions
GROUP BY validation_status, is_flagged;
```

**Find high-value flagged transactions**:
```sql
SELECT
    transaction_id,
    account_id,
    amount,
    merchant,
    transaction_timestamp
FROM iceberg.silver.transactions
WHERE is_flagged = true
ORDER BY amount DESC
LIMIT 20;
```

### Gold Layer Queries

**Daily account summaries**:
```sql
SELECT
    account_id,
    transaction_date,
    total_transactions,
    total_debits,
    total_credits,
    net_amount,
    flagged_transactions
FROM iceberg.gold.daily_account_summary
ORDER BY transaction_date DESC, net_amount DESC
LIMIT 20;
```

**Top accounts by activity**:
```sql
SELECT
    account_id,
    SUM(total_transactions) as total_txns,
    SUM(net_amount) as total_net,
    AVG(avg_transaction_amount) as overall_avg
FROM iceberg.gold.daily_account_summary
GROUP BY account_id
ORDER BY total_txns DESC
LIMIT 10;
```

**Merchant analytics**:
```sql
SELECT
    merchant,
    merchant_category,
    SUM(total_transactions) as total_txns,
    SUM(total_amount) as total_revenue,
    AVG(avg_amount) as avg_txn_amount,
    SUM(unique_accounts) as total_customers
FROM iceberg.gold.merchant_summary
GROUP BY merchant, merchant_category
ORDER BY total_revenue DESC
LIMIT 20;
```

**Top merchants by revenue**:
```sql
SELECT
    merchant,
    total_amount as revenue,
    total_transactions,
    unique_accounts as customers
FROM iceberg.gold.merchant_summary
WHERE transaction_date >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY total_amount DESC
LIMIT 10;
```

---

## Scheduling & Automation

### Option 1: Cron Job (Simple)

Run daily at 2 AM:

```bash
# Edit crontab
crontab -e

# Add this line:
0 2 * * * /home/user/onprem-streaming-processing-system/guides/data-ingestion-processing/scripts/run-batch-pipeline.sh >> /var/log/batch-pipeline.log 2>&1
```

### Option 2: Airflow (Production)

Create an Airflow DAG:

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'batch_aggregations',
    default_args=default_args,
    description='Process Bronze to Silver to Gold',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
)

batch_job = SparkSubmitOperator(
    task_id='run_batch_aggregations',
    application='/opt/bitnami/spark/jobs/batch_aggregations.py',
    conn_id='spark_default',
    packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2',
    dag=dag,
)
```

### Option 3: Manual Runs

Run whenever needed:
```bash
bash guides/data-ingestion-processing/scripts/run-batch-pipeline.sh
```

---

## Customization

### Modify Data Quality Rules

Edit `spark/jobs/batch_aggregations.py`:

```python
# Change flagging threshold
.withColumn(
    "is_flagged",
    when(col("amount") > 5000, True).otherwise(False)  # Changed from 10000
)

# Add more validation rules
.filter(col("currency") == "USD")  # Only USD transactions
.filter(col("merchant").isNotNull())  # Merchant must exist
```

### Add Custom Aggregations

Add new Gold table:

```python
def create_hourly_summary(spark):
    """Create hourly transaction summaries"""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.hourly_summary (
            hour TIMESTAMP,
            transaction_type STRING,
            total_transactions BIGINT,
            total_amount DECIMAL(18,2),
            avg_amount DECIMAL(18,2)
        )
        USING iceberg
    """)

    hourly_df = spark.sql("""
        SELECT
            date_trunc('hour', transaction_timestamp) as hour,
            transaction_type,
            COUNT(*) as total_transactions,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM local.silver.transactions
        GROUP BY 1, 2
    """)

    hourly_df.writeTo("local.gold.hourly_summary").append()
```

### Process Specific Date Range

Modify the script to process custom dates:

```python
# In bronze_to_silver function, change:
bronze_df = bronze_df.filter(
    (col("date_partition") >= "2024-01-01") &
    (col("date_partition") <= "2024-01-31")
)
```

---

## Monitoring & Troubleshooting

### Monitor Job Progress

**Spark UI**: http://localhost:8888
- View running/completed applications
- Check job stages and tasks
- Monitor executor usage
- View logs

### Check Logs

```bash
# Spark master logs
docker compose logs -f spark-master

# Filter for batch job
docker compose logs spark-master | grep -i "batch\|aggregation"
```

### Common Issues

#### Issue 1: No Data in Bronze Layer

**Error**: "No data in Bronze layer"

**Solution**:
```bash
# Check if streaming pipeline is running
make status

# Verify Bronze table exists
make shell-trino
> SHOW TABLES IN iceberg.bronze;

# Check record count
> SELECT COUNT(*) FROM iceberg.bronze.transactions;
```

#### Issue 2: Spark Job Fails

**Error**: "Table not found: local.bronze.transactions"

**Solution**:
```bash
# Ensure schemas exist
make shell-trino
> CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
> CREATE SCHEMA IF NOT EXISTS iceberg.silver;
> CREATE SCHEMA IF NOT EXISTS iceberg.gold;
```

#### Issue 3: No Records in Silver/Gold

**Check**:
```sql
-- Verify date partitions in Bronze
SELECT DISTINCT date_partition
FROM iceberg.bronze.transactions
ORDER BY date_partition DESC;
```

**Reason**: Batch job processes yesterday's data by default

**Solution**: Modify script to process today's data or run with custom date

#### Issue 4: Duplicate Records

**Check**:
```sql
SELECT transaction_id, COUNT(*)
FROM iceberg.silver.transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;
```

**Solution**: Re-run batch job (it deduplicates automatically)

### Verify Data Quality

**Check Silver layer quality**:
```sql
-- Should be 0 invalid records
SELECT COUNT(*)
FROM iceberg.silver.transactions
WHERE validation_status = 'INVALID';

-- Should be 0 negative amounts
SELECT COUNT(*)
FROM iceberg.silver.transactions
WHERE amount <= 0;

-- Check flagged transactions
SELECT
    is_flagged,
    COUNT(*) as count,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM iceberg.silver.transactions
GROUP BY is_flagged;
```

---

## Best Practices

### Data Processing

✅ **DO**:
- Run batch jobs during off-peak hours
- Process data incrementally by date
- Monitor job execution time
- Validate results after each run
- Keep Bronze layer immutable
- Document custom transformations

❌ **DON'T**:
- Don't delete Bronze data (source of truth)
- Don't skip validation steps
- Don't run batch jobs while streaming is writing
- Don't process entire history daily (use incremental)

### Performance

✅ **Optimize**:
- Partition tables by date
- Use appropriate file sizes (128MB target)
- Compact small files regularly
- Cache frequently accessed Silver tables
- Use pushdown predicates in queries

### Monitoring

✅ **Track**:
- Processing time trends
- Record counts per layer
- Data quality metrics
- Flagged transaction counts
- Failed validations

---

## Summary

The batch processing pipeline provides:

1. ✅ **Data Quality**: Automatic validation and cleaning
2. ✅ **Performance**: Pre-aggregated business metrics
3. ✅ **Organization**: Clear separation of raw vs. curated data
4. ✅ **Flexibility**: Easy to customize and extend
5. ✅ **Monitoring**: Full visibility into processing

**Complete Data Flow**:
```
Kafka → Bronze (Streaming) → Silver (Batch) → Gold (Batch) → BI Tools
```

**Next Steps**:
- Run your first batch job: `bash scripts/run-batch-pipeline.sh`
- Query processed data in Trino
- Customize transformations for your needs
- Schedule regular runs with cron or Airflow

---

## Related Documentation

- [BEGINNER_WALKTHROUGH.md](BEGINNER_WALKTHROUGH.md) - Get started with the platform
- [DATA_STREAMING_GUIDE.md](DATA_STREAMING_GUIDE.md) - Streaming data ingestion
- [Main README](../README.md) - All guides and examples
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) - System architecture
