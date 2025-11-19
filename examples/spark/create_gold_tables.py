"""
Gold Layer Aggregation Example
================================
Purpose: Create business-ready aggregated tables from Silver data
Run: docker cp examples/spark/create_gold_tables.py spark-master:/tmp/
     docker exec spark-master spark-submit \
       --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
       /tmp/create_gold_tables.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark
spark = SparkSession.builder \
    .appName("Create Gold Tables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("\n" + "="*80)
print("CREATING GOLD LAYER TABLES")
print("="*80 + "\n")

# Read Silver data
print("📖 Reading Silver data...")
silver_df = spark.table("iceberg.silver.transactions")
record_count = silver_df.count()
print(f"   ✓ Loaded {record_count:,} records from Silver\n")

# =============================================================================
# Gold Table 1: Daily Account Summary
# =============================================================================
print("🏆 Creating Gold Table 1: daily_account_summary")
print("-" * 80)

daily_account_summary = silver_df \
    .groupBy("account_id", "customer_name", "date_partition") \
    .agg(
        # Transaction counts
        count("*").alias("transaction_count"),
        countDistinct("merchant").alias("unique_merchants"),
        countDistinct("transaction_type").alias("unique_transaction_types"),

        # Amount aggregations
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount"),
        round(max("amount"), 2).alias("max_amount"),
        round(min("amount"), 2).alias("min_amount"),

        # By transaction type
        round(sum(when(col("transaction_type") == "PURCHASE", col("amount")).otherwise(0)), 2).alias("purchase_amount"),
        round(sum(when(col("transaction_type") == "WITHDRAWAL", col("amount")).otherwise(0)), 2).alias("withdrawal_amount"),
        round(sum(when(col("transaction_type") == "DEPOSIT", col("amount")).otherwise(0)), 2).alias("deposit_amount"),
        round(sum(when(col("transaction_type") == "TRANSFER", col("amount")).otherwise(0)), 2).alias("transfer_amount"),

        # Risk indicators
        sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_flag_count"),
        sum(when(col("is_high_value"), 1).otherwise(0)).alias("high_value_count"),

        # Timestamps
        min("timestamp").alias("first_transaction_time"),
        max("timestamp").alias("last_transaction_time")
    ) \
    .withColumnRenamed("date_partition", "transaction_date") \
    .withColumn("created_at", current_timestamp())

# Write to Gold
daily_account_summary.writeTo("iceberg.gold.daily_account_summary") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.parquet.compression-codec", "snappy") \
    .createOrReplace()

summary_count = daily_account_summary.count()
print(f"   ✓ Created table with {summary_count:,} account-day records")
print(f"   ✓ Query: SELECT * FROM iceberg.gold.daily_account_summary;\n")

# =============================================================================
# Gold Table 2: Merchant Performance
# =============================================================================
print("🏆 Creating Gold Table 2: merchant_performance")
print("-" * 80)

merchant_performance = silver_df \
    .groupBy("merchant", "date_partition") \
    .agg(
        # Volume metrics
        count("*").alias("transaction_volume"),
        countDistinct("account_id").alias("unique_customers"),

        # Revenue metrics
        round(sum("amount"), 2).alias("total_revenue"),
        round(avg("amount"), 2).alias("avg_transaction_value"),
        round(max("amount"), 2).alias("largest_transaction"),

        # Customer metrics
        round(sum("amount") / countDistinct("account_id"), 2).alias("revenue_per_customer"),
        round(count("*") / countDistinct("account_id"), 2).alias("transactions_per_customer"),

        # Risk metrics
        sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count"),
        round(sum(when(col("is_fraud"), col("amount")).otherwise(0)), 2).alias("fraud_amount"),

        # Success rate
        sum(when(col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
        sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed_count")
    ) \
    .withColumn("success_rate",
        round(100.0 * col("completed_count") / (col("completed_count") + col("failed_count")), 2)
    ) \
    .withColumnRenamed("date_partition", "transaction_date") \
    .withColumn("created_at", current_timestamp())

merchant_performance.writeTo("iceberg.gold.merchant_performance") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

merchant_count = merchant_performance.count()
print(f"   ✓ Created table with {merchant_count:,} merchant-day records")
print(f"   ✓ Query: SELECT * FROM iceberg.gold.merchant_performance;\n")

# =============================================================================
# Gold Table 3: Hourly Trends
# =============================================================================
print("🏆 Creating Gold Table 3: hourly_trends")
print("-" * 80)

hourly_trends = silver_df \
    .withColumn("hour_timestamp", date_trunc('hour', col("timestamp"))) \
    .groupBy("hour_timestamp") \
    .agg(
        # Transaction metrics
        count("*").alias("transaction_count"),
        countDistinct("account_id").alias("active_accounts"),
        countDistinct("merchant").alias("active_merchants"),

        # Amount metrics
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount"),

        # By type
        sum(when(col("transaction_type") == "PURCHASE", 1).otherwise(0)).alias("purchases"),
        sum(when(col("transaction_type") == "WITHDRAWAL", 1).otherwise(0)).alias("withdrawals"),
        sum(when(col("transaction_type") == "DEPOSIT", 1).otherwise(0)).alias("deposits"),
        sum(when(col("transaction_type") == "TRANSFER", 1).otherwise(0)).alias("transfers"),
        sum(when(col("transaction_type") == "PAYMENT", 1).otherwise(0)).alias("payments"),

        # Status
        sum(when(col("status") == "COMPLETED", 1).otherwise(0)).alias("completed"),
        sum(when(col("status") == "PENDING", 1).otherwise(0)).alias("pending"),
        sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed"),

        # Fraud
        sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_flags")
    ) \
    .withColumn("created_at", current_timestamp())

hourly_trends.writeTo("iceberg.gold.hourly_trends") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

hourly_count = hourly_trends.count()
print(f"   ✓ Created table with {hourly_count:,} hourly records")
print(f"   ✓ Query: SELECT * FROM iceberg.gold.hourly_trends;\n")

# =============================================================================
# Gold Table 4: Geographic Summary
# =============================================================================
print("🏆 Creating Gold Table 4: geographic_summary")
print("-" * 80)

geographic_summary = silver_df \
    .groupBy("location.city", "date_partition") \
    .agg(
        count("*").alias("transaction_count"),
        countDistinct("account_id").alias("unique_customers"),
        countDistinct("merchant").alias("unique_merchants"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount"),
        sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count")
    ) \
    .withColumnRenamed("city", "city_name") \
    .withColumnRenamed("date_partition", "transaction_date") \
    .withColumn("created_at", current_timestamp())

geographic_summary.writeTo("iceberg.gold.geographic_summary") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

geo_count = geographic_summary.count()
print(f"   ✓ Created table with {geo_count:,} city-day records")
print(f"   ✓ Query: SELECT * FROM iceberg.gold.geographic_summary;\n")

# =============================================================================
# Verification Queries
# =============================================================================
print("="*80)
print("📊 GOLD LAYER SUMMARY")
print("="*80 + "\n")

# Show sample from each table
print("Sample: Daily Account Summary (Top 5 Accounts)")
print("-" * 80)
spark.sql("""
    SELECT
        account_id,
        transaction_date,
        transaction_count,
        total_amount,
        unique_merchants
    FROM iceberg.gold.daily_account_summary
    ORDER BY total_amount DESC
    LIMIT 5
""").show(truncate=False)

print("\nSample: Merchant Performance (Top 5 Merchants)")
print("-" * 80)
spark.sql("""
    SELECT
        merchant,
        transaction_date,
        transaction_volume,
        total_revenue,
        unique_customers
    FROM iceberg.gold.merchant_performance
    ORDER BY total_revenue DESC
    LIMIT 5
""").show(truncate=False)

print("\nSample: Hourly Trends (Latest 5 Hours)")
print("-" * 80)
spark.sql("""
    SELECT
        hour_timestamp,
        transaction_count,
        active_accounts,
        total_amount
    FROM iceberg.gold.hourly_trends
    ORDER BY hour_timestamp DESC
    LIMIT 5
""").show(truncate=False)

# =============================================================================
# Summary
# =============================================================================
print("="*80)
print("✅ GOLD LAYER CREATED SUCCESSFULLY!")
print("="*80)
print(f"\nCreated 4 Gold tables:")
print(f"  1. daily_account_summary:   {summary_count:,} records")
print(f"  2. merchant_performance:    {merchant_count:,} records")
print(f"  3. hourly_trends:           {hourly_count:,} records")
print(f"  4. geographic_summary:      {geo_count:,} records")
print(f"\nThese tables are optimized for BI dashboards and reporting.")
print(f"Query them via Trino or connect Superset for visualization!\n")

spark.stop()
