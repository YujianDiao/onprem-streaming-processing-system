"""
Simple ETL Example: Bronze to Silver Transformation
====================================================
Purpose: Transform raw Bronze data into curated Silver layer
Run: docker cp examples/spark/simple_etl.py spark-master:/tmp/
     docker exec spark-master spark-submit \
       --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
       /tmp/simple_etl.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark with Iceberg support
spark = SparkSession.builder \
    .appName("Simple Bronze to Silver ETL") \
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
print("SIMPLE ETL: Bronze → Silver")
print("="*80 + "\n")

# =============================================================================
# Step 1: Read Bronze Data
# =============================================================================
print("📖 Step 1: Reading Bronze data...")
bronze_df = spark.table("iceberg.bronze.transactions")
initial_count = bronze_df.count()
print(f"   ✓ Loaded {initial_count:,} records from Bronze\n")

# =============================================================================
# Step 2: Data Quality - Deduplication
# =============================================================================
print("🧹 Step 2: Deduplicating records...")
# Keep the latest record for each transaction_id
window = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
deduped_df = bronze_df \
    .withColumn("row_num", row_number().over(window)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

dedup_count = deduped_df.count()
duplicates_removed = initial_count - dedup_count
print(f"   ✓ Removed {duplicates_removed} duplicates")
print(f"   ✓ {dedup_count:,} unique records remaining\n")

# =============================================================================
# Step 3: Data Validation
# =============================================================================
print("✅ Step 3: Validating data quality...")
validated_df = deduped_df \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .filter(col("status").isin("COMPLETED", "PENDING", "FAILED"))

validation_count = validated_df.count()
invalid_removed = dedup_count - validation_count
print(f"   ✓ Removed {invalid_removed} invalid records")
print(f"   ✓ {validation_count:,} valid records remaining\n")

# =============================================================================
# Step 4: Type Casting & Enrichment
# =============================================================================
print("🔄 Step 4: Transforming and enriching data...")
silver_df = validated_df \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .withColumn("amount", col("amount").cast("decimal(18,2)")) \
    .withColumn("is_high_value", col("amount") > 10000) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .withColumn("processed_date", current_date()) \
    .withColumn("hour_of_day", hour(col("timestamp"))) \
    .withColumn("day_of_week", dayofweek(col("timestamp"))) \
    .withColumn("is_weekend", dayofweek(col("timestamp")).isin(1, 7)) \
    .withColumn("transaction_category",
        when(col("amount") < 100, "Small")
        .when(col("amount") < 1000, "Medium")
        .when(col("amount") < 10000, "Large")
        .otherwise("Extra Large")
    )

print("   ✓ Applied transformations:")
print("      - Cast timestamp and amount to proper types")
print("      - Added is_high_value flag")
print("      - Added temporal fields (hour, day of week)")
print("      - Categorized transactions by amount\n")

# =============================================================================
# Step 5: Write to Silver Table
# =============================================================================
print("💾 Step 5: Writing to Silver table...")
silver_df.writeTo("iceberg.silver.transactions") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.parquet.compression-codec", "gzip") \
    .createOrReplace()

print(f"   ✓ Successfully wrote {silver_df.count():,} records to Silver\n")

# =============================================================================
# Step 6: Data Quality Report
# =============================================================================
print("="*80)
print("📊 DATA QUALITY REPORT")
print("="*80 + "\n")

# Overall statistics
stats = silver_df.agg(
    count("*").alias("total_records"),
    countDistinct("account_id").alias("unique_accounts"),
    countDistinct("merchant").alias("unique_merchants"),
    round(sum("amount"), 2).alias("total_amount"),
    round(avg("amount"), 2).alias("avg_amount"),
    round(min("amount"), 2).alias("min_amount"),
    round(max("amount"), 2).alias("max_amount")
).collect()[0]

print("Overall Statistics:")
print(f"  • Total Records: {stats['total_records']:,}")
print(f"  • Unique Accounts: {stats['unique_accounts']:,}")
print(f"  • Unique Merchants: {stats['unique_merchants']:,}")
print(f"  • Total Amount: ${stats['total_amount']:,.2f}")
print(f"  • Average Amount: ${stats['avg_amount']:,.2f}")
print(f"  • Min Amount: ${stats['min_amount']:,.2f}")
print(f"  • Max Amount: ${stats['max_amount']:,.2f}\n")

# Transaction type breakdown
print("Transaction Types:")
type_breakdown = silver_df.groupBy("transaction_type") \
    .agg(
        count("*").alias("count"),
        round(sum("amount"), 2).alias("total")
    ) \
    .orderBy(col("count").desc()) \
    .collect()

for row in type_breakdown:
    print(f"  • {row['transaction_type']}: {row['count']:,} transactions (${row['total']:,.2f})")
print()

# Category breakdown
print("Transaction Categories:")
category_breakdown = silver_df.groupBy("transaction_category") \
    .agg(count("*").alias("count")) \
    .orderBy(
        when(col("transaction_category") == "Small", 1)
        .when(col("transaction_category") == "Medium", 2)
        .when(col("transaction_category") == "Large", 3)
        .otherwise(4)
    ) \
    .collect()

for row in category_breakdown:
    pct = 100.0 * row['count'] / stats['total_records']
    print(f"  • {row['transaction_category']}: {row['count']:,} ({pct:.1f}%)")
print()

# High-value transactions
high_value_count = silver_df.filter(col("is_high_value")).count()
print(f"High-Value Transactions (>$10,000): {high_value_count:,}")

# Fraud flags
fraud_count = silver_df.filter(col("is_fraud")).count()
fraud_pct = 100.0 * fraud_count / stats['total_records']
print(f"Flagged Fraud: {fraud_count:,} ({fraud_pct:.2f}%)")

# Data quality score
quality_score = 100.0 * validation_count / initial_count
print(f"\n📈 Data Quality Score: {quality_score:.2f}%")
print(f"   ({validation_count:,} valid / {initial_count:,} total)")

# =============================================================================
# Summary
# =============================================================================
print("\n" + "="*80)
print("✅ ETL COMPLETED SUCCESSFULLY!")
print("="*80)
print(f"\nSummary:")
print(f"  • Input (Bronze):  {initial_count:,} records")
print(f"  • Duplicates:      -{duplicates_removed:,} records")
print(f"  • Invalid Data:    -{invalid_removed:,} records")
print(f"  • Output (Silver): {validation_count:,} records")
print(f"\nYou can now query the Silver table:")
print(f"  • Via Trino: SELECT * FROM iceberg.silver.transactions LIMIT 10;")
print(f"  • Via Spark: spark.table('iceberg.silver.transactions').show()")
print()

spark.stop()
