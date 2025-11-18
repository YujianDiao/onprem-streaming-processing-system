# Batch Aggregations Pipeline Update

## Summary

Updated the batch aggregations pipeline (`spark/jobs/batch_aggregations.py`) to work with the fixed Hive Metastore infrastructure. The pipeline now correctly processes data through Bronze → Silver → Gold layers using the Iceberg catalog with Hive Metastore.

---

## Issues Fixed

### Issue 1: Wrong Catalog Configuration

**Problem:**
- Used `local` catalog with `hadoop` type instead of `iceberg` catalog with `hive` type
- Would not connect to Hive Metastore
- Incompatible with the infrastructure setup

**Location:** Lines 17-31 (original)

**Fix:**
Changed from Hadoop catalog to Hive-based Iceberg catalog to match the streaming pipeline configuration.

**Before:**
```python
.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
.config("spark.sql.catalog.local.type", "hadoop") \
.config("spark.sql.catalog.local.warehouse", "s3a://lakehouse/")
```

**After:**
```python
.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
.config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
.config("spark.sql.catalog.iceberg.type", "hive") \
.config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
.config("spark.sql.catalog.iceberg.warehouse", "s3a://lakehouse/")
```

---

### Issue 2: Inconsistent Catalog References

**Problem:**
- All table references used `local.bronze.*`, `local.silver.*`, `local.gold.*`
- Bronze table was created under `iceberg` catalog, not `local`
- Would fail with "table not found" errors

**Locations:** Lines 99, 145, 161, 200, 213, 235, 241

**Fix:**
Updated all table references to use `iceberg` catalog:
- `local.bronze.transactions` → `iceberg.bronze.transactions`
- `local.silver.transactions` → `iceberg.silver.transactions`
- `local.gold.*` → `iceberg.gold.*`

---

### Issue 3: Schema Mismatch - Missing Fields

**Problem:**
- Silver table schema referenced fields that don't exist in Bronze:
  - `merchant_category` (Bronze doesn't have this field)
  - `transaction_timestamp` (Bronze has `timestamp`, not `transaction_timestamp`)

**Location:** Lines 46-47, 129-130

**Fix:**

**Silver Table Schema (Updated):**
- Removed `merchant_category` field (not in Bronze)
- Changed `transaction_timestamp` to use derived value from Bronze `timestamp` field

**Before:**
```python
merchant_category STRING,
transaction_timestamp TIMESTAMP,
```

**After:**
```python
# Removed merchant_category
transaction_timestamp TIMESTAMP,  # Derived: to_timestamp(col("timestamp"))
```

**Transformation Code:**
```python
.withColumn("transaction_timestamp", to_timestamp(col("timestamp"))) \
.select(
    "transaction_id",
    "account_id",
    "transaction_type",
    col("amount").cast("decimal(18,2)").alias("amount"),
    "currency",
    "merchant",
    "transaction_timestamp",  # From derived column
    "status",
    "is_flagged",
    "validation_status",
    "processing_timestamp",
    col("date_partition").cast("date").alias("date_partition")
)
```

---

### Issue 4: Incorrect Transaction Type Logic

**Problem:**
- Gold aggregations used generic `DEBIT`/`CREDIT` transaction types
- Actual Bronze data has: `PURCHASE`, `WITHDRAWAL`, `DEPOSIT`, `TRANSFER`, `PAYMENT`
- Would result in incorrect or zero aggregations

**Location:** Lines 174-182

**Fix:**
Updated aggregation logic to match actual transaction types in the data.

**Transaction Type Mapping:**
- **Debits**: `WITHDRAWAL`, `PAYMENT`, `PURCHASE`
- **Credits**: `DEPOSIT`, `TRANSFER`

**Before:**
```python
sum(when(col("transaction_type") == "DEBIT", col("amount")).otherwise(0))
sum(when(col("transaction_type") == "CREDIT", col("amount")).otherwise(0))
```

**After:**
```python
sum(when(col("transaction_type").isin(["WITHDRAWAL", "PAYMENT", "PURCHASE"]), col("amount")).otherwise(0))
sum(when(col("transaction_type").isin(["DEPOSIT", "TRANSFER"]), col("amount")).otherwise(0))
```

---

## New Infrastructure Setup

### Created Schemas

Created `silver` and `gold` schemas in the Iceberg catalog:

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location='s3a://lakehouse/silver');

CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location='s3a://lakehouse/gold');
```

### Created Submission Script

**File:** `submit-batch-job.sh`

Features:
- Synchronous execution (waits for job completion)
- Uses same Hive Metastore connection as streaming job
- Includes all necessary Iceberg and S3 packages

```bash
#!/bin/bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.catalog.iceberg.type=hive \
  --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
  [... S3 configs ...] \
  /opt/spark/jobs/batch_aggregations.py
```

---

## Data Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Batch Aggregations Pipeline                       │
└─────────────────────────────────────────────────────────────────────┘

Bronze Layer (iceberg.bronze.transactions)
  │ Source: Streaming job from Kafka
  │ Schema: Raw transaction data
  │ Partition: By date_partition (string)
  ▼

Silver Layer (iceberg.silver.transactions)
  │ Transformations:
  │   - Deduplication by transaction_id
  │   - Data quality filters (amount > 0)
  │   - Status validation (COMPLETED/PENDING/FAILED)
  │   - Fraud flagging (amount > $10,000)
  │   - Timestamp parsing
  │ Schema: Cleaned/validated transactions
  │ Partition: By days(date_partition)
  ▼

Gold Layer (iceberg.gold.daily_account_summary)
  │ Aggregations:
  │   - Total transactions per account per day
  │   - Total debits/credits
  │   - Net amount
  │   - Min/max/avg transaction amounts
  │   - Distinct merchant count
  │   - Flagged transaction count
  │ Schema: Business-level metrics
  │ Partition: By days(transaction_date)

Gold Layer (iceberg.gold.merchant_summary)
  │ Aggregations:
  │   - Merchant transaction volume
  │   - Total/average amounts per merchant
  │   - Unique account counts
  │ Schema: Merchant analytics
  │ Partition: By days(transaction_date)
```

---

## Modified Files Summary

### spark/jobs/batch_aggregations.py

**Lines 17-33**: Updated Spark session configuration
- Changed from `local` Hadoop catalog to `iceberg` Hive catalog
- Added Hive Metastore URI configuration

**Lines 41, 69**: Updated table creation DDL
- Changed catalog from `local` to `iceberg`
- Removed `merchant_category` from silver schema

**Lines 99-150**: Updated bronze_to_silver transformation
- Changed table reference from `local.bronze` to `iceberg.bronze`
- Added `transaction_timestamp` derivation from `timestamp` field
- Removed `merchant_category` selection
- Added explicit decimal casting for amount field
- Added date casting for date_partition

**Lines 161-205**: Updated silver_to_gold transformation
- Changed table references to use `iceberg` catalog
- Updated transaction type logic for actual data values
- Updated DEBIT/CREDIT mapping to WITHDRAWAL/PAYMENT/PURCHASE and DEPOSIT/TRANSFER

**Lines 213-245**: Updated merchant analytics
- Changed table references to use `iceberg` catalog
- Removed `merchant_category` field

### New Files

**submit-batch-job.sh**
- Batch job submission script
- Synchronous execution (non-detached)
- Matches streaming job configuration

---

## Usage

### Running the Batch Job

```bash
# Make script executable (already done)
chmod +x submit-batch-job.sh

# Run batch aggregations
./submit-batch-job.sh
```

### Verify Results

```bash
# Check Silver layer
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.silver.transactions;"

# Check Gold layer - Daily summary
docker exec trino trino --execute \
  "SELECT * FROM iceberg.gold.daily_account_summary LIMIT 5;"

# Check Gold layer - Merchant summary
docker exec trino trino --execute \
  "SELECT * FROM iceberg.gold.merchant_summary
   ORDER BY total_amount DESC LIMIT 10;"
```

### Data Quality Checks

```bash
# Verify deduplication
docker exec trino trino --execute \
  "SELECT COUNT(*) as total, COUNT(DISTINCT transaction_id) as unique
   FROM iceberg.silver.transactions;"

# Check flagged transactions
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.silver.transactions
   WHERE is_flagged = true;"

# Validate amounts
docker exec trino trino --execute \
  "SELECT MIN(amount), MAX(amount), AVG(amount)
   FROM iceberg.silver.transactions;"
```

---

## Testing Recommendations

### 1. Test Bronze to Silver Processing

```python
# In batch_aggregations.py, add before line 246:
logger.info("Testing bronze to silver transformation")
test_df = spark.sql("""
    SELECT COUNT(*) as bronze_count
    FROM iceberg.bronze.transactions
    WHERE date_partition = DATE_SUB(CURRENT_DATE(), 1)
""")
test_df.show()
```

### 2. Test Data Quality Rules

Verify these rules are applied:
- Only transactions with amount > 0
- Only status in (COMPLETED, PENDING, FAILED)
- Duplicates removed by transaction_id
- High-value transactions flagged (> $10,000)

### 3. Test Aggregation Logic

Verify aggregations match manual calculations:

```sql
-- Manual verification query
SELECT
    account_id,
    COUNT(*) as tx_count,
    SUM(CASE WHEN transaction_type IN ('WITHDRAWAL','PAYMENT','PURCHASE')
        THEN amount ELSE 0 END) as debits,
    SUM(CASE WHEN transaction_type IN ('DEPOSIT','TRANSFER')
        THEN amount ELSE 0 END) as credits
FROM iceberg.silver.transactions
WHERE date_partition = DATE_SUB(CURRENT_DATE(), 1)
GROUP BY account_id
LIMIT 5;
```

---

## Performance Considerations

### Current Configuration
- **Processing Mode**: Batch (daily)
- **Default Filter**: Yesterday's data (DATE_SUB(CURRENT_DATE(), 1))
- **Partitioning**: Days-based partitioning for time-series queries
- **File Format**: Parquet with GZIP compression

### Optimization Opportunities

1. **Date Range Processing**
   ```python
   # Process specific date
   bronze_to_silver(spark, processing_date="2025-11-16")

   # Or process date range (requires code modification)
   ```

2. **Incremental Processing**
   - Add watermark tracking to avoid reprocessing
   - Use Iceberg snapshots for incremental reads

3. **Parallel Processing**
   - Increase Spark executor count for large datasets
   - Adjust partition size for optimal parallelism

---

## Troubleshooting

### Issue: "Table not found" errors

**Check:**
```bash
# Verify bronze table exists
docker exec trino trino --execute "SHOW TABLES IN iceberg.bronze;"

# Verify schemas exist
docker exec trino trino --execute "SHOW SCHEMAS IN iceberg;"
```

**Fix:**
Ensure streaming job has created bronze.transactions table first.

---

### Issue: "Column not found: merchant_category"

**This was fixed** - if you still see this error:
1. Pull latest version of `batch_aggregations.py`
2. Verify line 129 does NOT reference `merchant_category`

---

### Issue: Zero records in aggregations

**Check transaction types:**
```bash
docker exec trino trino --execute \
  "SELECT transaction_type, COUNT(*)
   FROM iceberg.bronze.transactions
   GROUP BY transaction_type;"
```

**Expected:** PURCHASE, WITHDRAWAL, DEPOSIT, TRANSFER, PAYMENT

---

### Issue: Aggregation values seem incorrect

**Verify mapping:**
- Debits should include: WITHDRAWAL, PAYMENT, PURCHASE
- Credits should include: DEPOSIT, TRANSFER

**Debug query:**
```sql
SELECT
    transaction_type,
    SUM(amount) as total,
    COUNT(*) as count
FROM iceberg.silver.transactions
GROUP BY transaction_type;
```

---

## Next Steps

1. **Schedule Batch Job**
   - Set up cron or Airflow DAG for daily execution
   - Run after streaming job has processed the previous day

2. **Add Data Quality Tests**
   - Implement Great Expectations or similar framework
   - Add data validation before writing to Silver/Gold

3. **Extend Gold Layer**
   - Add more business metrics
   - Create time-series aggregations (weekly, monthly)
   - Add customer segmentation tables

4. **Monitoring**
   - Track record counts at each layer
   - Monitor processing time
   - Alert on data quality failures

---

**Document Version**: 1.0
**Last Updated**: 2025-11-17
**Related**: STREAMING_PIPELINE_FIX.md
**Status**: Ready for Testing
