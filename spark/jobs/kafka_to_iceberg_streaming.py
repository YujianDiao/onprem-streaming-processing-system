"""
Kafka to Iceberg Streaming Job
Reads banking transaction data from Kafka and writes to Iceberg tables in MinIO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session with Iceberg and Kafka configurations"""
    return SparkSession.builder \
        .appName("KafkaToIcebergStreaming") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
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


def define_transaction_schema():
    """Define schema for banking transaction data"""
    return StructType([
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


def create_iceberg_table_if_not_exists(spark, table_name, schema):
    """Create Iceberg table if it doesn't exist"""
    try:
        spark.sql(f"DESCRIBE TABLE local.{table_name}")
        logger.info(f"Table local.{table_name} already exists")
    except Exception:
        logger.info(f"Creating table local.{table_name}")

        # Create bronze layer table (raw data)
        if "bronze" in table_name:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS local.{table_name} (
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
            """)
        logger.info(f"Table local.{table_name} created successfully")


def process_kafka_stream(spark):
    """Read from Kafka and write to Iceberg"""

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29093,kafka-3:29094") \
        .option("subscribe", "banking.transactions.raw") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info("Connected to Kafka topic: banking.transactions.raw")

    # Parse JSON data from Kafka
    transaction_schema = define_transaction_schema()

    parsed_df = kafka_df \
        .select(
            from_json(col("value").cast("string"), transaction_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp")

    # Add processing metadata
    enriched_df = parsed_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("date_partition", to_date(col("transaction_timestamp")))

    # Create Iceberg table if not exists
    create_iceberg_table_if_not_exists(
        spark,
        "bronze.transactions",
        transaction_schema
    )

    # Write to Iceberg table (Bronze layer - raw data)
    query = enriched_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("path", "s3a://lakehouse/bronze/transactions") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze_transactions") \
        .option("fanout-enabled", "true") \
        .trigger(processingTime='30 seconds') \
        .start()

    logger.info("Streaming query started - writing to Iceberg")

    return query


def main():
    """Main execution function"""
    logger.info("Starting Kafka to Iceberg streaming job")

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Process stream
        query = process_kafka_stream(spark)

        # Wait for termination
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in streaming job: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
