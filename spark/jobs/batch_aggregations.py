"""
Batch Aggregations Job
Reads from Bronze Iceberg tables, performs transformations and aggregations,
and writes to Silver and Gold layers
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session with Iceberg configurations"""
    return SparkSession.builder \
        .appName("BatchAggregations") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://lakehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()


def create_silver_table(spark):
    """Create Silver layer table for cleaned/validated transactions"""
    logger.info("Creating Silver layer table")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.transactions (
            transaction_id STRING,
            account_id STRING,
            transaction_type STRING,
            amount DECIMAL(18,2),
            currency STRING,
            merchant STRING,
            merchant_category STRING,
            transaction_timestamp TIMESTAMP,
            status STRING,
            is_flagged BOOLEAN,
            validation_status STRING,
            processing_timestamp TIMESTAMP,
            date_partition DATE
        )
        USING iceberg
        PARTITIONED BY (days(date_partition))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.metadata.compression-codec' = 'gzip'
        )
    """)


def create_gold_table(spark):
    """Create Gold layer table for business-level aggregations"""
    logger.info("Creating Gold layer table")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.daily_account_summary (
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
        USING iceberg
        PARTITIONED BY (days(transaction_date))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.metadata.compression-codec' = 'gzip'
        )
    """)


def bronze_to_silver(spark, processing_date=None):
    """
    Transform Bronze layer data to Silver layer
    Apply data quality rules, deduplication, and validation
    """
    logger.info("Processing Bronze to Silver transformation")

    # Read from Bronze layer
    bronze_df = spark.table("local.bronze.transactions")

    # Filter for specific date if provided, else process yesterday
    if processing_date:
        bronze_df = bronze_df.filter(col("date_partition") == processing_date)
    else:
        bronze_df = bronze_df.filter(
            col("date_partition") == date_sub(current_date(), 1)
        )

    # Data Quality Checks and Transformations
    silver_df = bronze_df \
        .dropDuplicates(["transaction_id"]) \
        .filter(col("amount") > 0) \
        .filter(col("status").isin(["COMPLETED", "PENDING", "FAILED"])) \
        .withColumn(
            "is_flagged",
            when(col("amount") > 10000, True).otherwise(False)
        ) \
        .withColumn(
            "validation_status",
            when(col("amount") > 0, "VALID").otherwise("INVALID")
        ) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .select(
            "transaction_id",
            "account_id",
            "transaction_type",
            "amount",
            "currency",
            "merchant",
            "merchant_category",
            "transaction_timestamp",
            "status",
            "is_flagged",
            "validation_status",
            "processing_timestamp",
            "date_partition"
        )

    # Write to Silver layer
    logger.info(f"Writing {silver_df.count()} records to Silver layer")

    silver_df.writeTo("local.silver.transactions") \
        .using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .append()

    return silver_df


def silver_to_gold(spark, processing_date=None):
    """
    Create business-level aggregations from Silver layer
    Generate daily account summaries
    """
    logger.info("Processing Silver to Gold transformation")

    # Read from Silver layer
    silver_df = spark.table("local.silver.transactions")

    # Filter for specific date
    if processing_date:
        silver_df = silver_df.filter(col("date_partition") == processing_date)
    else:
        silver_df = silver_df.filter(
            col("date_partition") == date_sub(current_date(), 1)
        )

    # Create aggregations
    gold_df = silver_df \
        .groupBy("account_id", "date_partition") \
        .agg(
            count("*").alias("total_transactions"),
            sum(
                when(col("transaction_type") == "DEBIT", col("amount")).otherwise(0)
            ).alias("total_debits"),
            sum(
                when(col("transaction_type") == "CREDIT", col("amount")).otherwise(0)
            ).alias("total_credits"),
            sum(
                when(col("transaction_type") == "CREDIT", col("amount"))
                .otherwise(-col("amount"))
            ).alias("net_amount"),
            avg("amount").alias("avg_transaction_amount"),
            max("amount").alias("max_transaction_amount"),
            min("amount").alias("min_transaction_amount"),
            countDistinct("merchant").alias("distinct_merchants"),
            sum(when(col("is_flagged"), 1).otherwise(0)).alias("flagged_transactions")
        ) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumnRenamed("date_partition", "transaction_date")

    # Write to Gold layer
    logger.info(f"Writing {gold_df.count()} records to Gold layer")

    gold_df.writeTo("local.gold.daily_account_summary") \
        .using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .append()

    return gold_df


def generate_merchant_analytics(spark):
    """Generate merchant-level analytics"""
    logger.info("Generating merchant analytics")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.merchant_summary (
            merchant STRING,
            merchant_category STRING,
            transaction_date DATE,
            total_transactions BIGINT,
            total_amount DECIMAL(18,2),
            avg_amount DECIMAL(18,2),
            unique_accounts BIGINT,
            processing_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(transaction_date))
    """)

    merchant_df = spark.sql("""
        SELECT
            merchant,
            merchant_category,
            date_partition as transaction_date,
            COUNT(*) as total_transactions,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            COUNT(DISTINCT account_id) as unique_accounts,
            CURRENT_TIMESTAMP() as processing_timestamp
        FROM local.silver.transactions
        WHERE date_partition = DATE_SUB(CURRENT_DATE(), 1)
            AND merchant IS NOT NULL
        GROUP BY merchant, merchant_category, date_partition
    """)

    merchant_df.writeTo("local.gold.merchant_summary") \
        .using("iceberg") \
        .append()

    logger.info(f"Merchant analytics completed: {merchant_df.count()} records")


def main():
    """Main execution function"""
    logger.info("Starting Batch Aggregations job")

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Create tables if they don't exist
        create_silver_table(spark)
        create_gold_table(spark)

        # Process data through layers
        silver_df = bronze_to_silver(spark)
        gold_df = silver_to_gold(spark)

        # Generate additional analytics
        generate_merchant_analytics(spark)

        logger.info("Batch aggregations completed successfully")

        # Show sample statistics
        logger.info(f"Silver layer records: {silver_df.count()}")
        logger.info(f"Gold layer records: {gold_df.count()}")

    except Exception as e:
        logger.error(f"Error in batch aggregations: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
