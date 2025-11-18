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
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://lakehouse/") \
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
        StructField("timestamp", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("merchant", StringType(), True),
        StructField("location", StructType([
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ]), True),
        StructField("customer", StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True)
        ]), True),
        StructField("is_fraud", BooleanType(), False),
        StructField("status", StringType(), False),
        StructField("metadata", StructType([
            StructField("device_type", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("session_id", StringType(), True)
        ]), True)
    ])


def create_iceberg_table_if_not_exists(spark, table_name, schema):
    """Create Iceberg table if it doesn't exist"""
    logger.info(f"Ensuring table iceberg.{table_name} exists")

    # Create bronze layer table (raw data)
    if "bronze" in table_name:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS iceberg.{table_name} (
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
                date_partition STRING
            )
            USING iceberg
            PARTITIONED BY (date_partition)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.metadata.compression-codec' = 'gzip'
            )
        """)
    logger.info(f"Table iceberg.{table_name} created successfully")


def write_batch_to_iceberg(batch_df, batch_id):
    """Write each batch to Iceberg using batch mode"""
    if batch_df.count() > 0:
        logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")

        # Write to Iceberg table using explicit S3 path
        # This bypasses database location and uses the direct S3 path
        batch_df.write \
            .format("iceberg") \
            .mode("append") \
            .option("fanout-enabled", "true") \
            .option("write.format.default", "parquet") \
            .option("write.metadata.compression-codec", "gzip") \
            .option("path", "s3a://lakehouse/bronze/transactions") \
            .partitionBy("date_partition") \
            .saveAsTable("iceberg.bronze.transactions")

        logger.info(f"Batch {batch_id} written successfully")
    else:
        logger.info(f"Batch {batch_id} is empty, skipping")


def process_kafka_stream(spark):
    """Read from Kafka and write to Iceberg using foreachBatch"""

    # Read from Kafka
    logger.info("Connecting to Kafka...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29092,kafka-3:29092") \
        .option("subscribe", "banking.transactions.raw") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.session.timeout.ms", "30000") \
        .option("kafka.request.timeout.ms", "40000") \
        .option("kafka.default.api.timeout.ms", "60000") \
        .option("maxOffsetsPerTrigger", "10000") \
        .load()

    logger.info("Connected to Kafka topic: banking.transactions.raw")

    # Parse JSON data from Kafka
    transaction_schema = define_transaction_schema()

    parsed_df = kafka_df \
        .select(
            from_json(col("value").cast("string"), transaction_schema).alias("data")
        ) \
        .select("data.*")

    # Add processing metadata
    enriched_df = parsed_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("date_partition", date_format(to_date(col("timestamp")), "yyyy-MM-dd"))

    # NOTE: Do NOT pre-create table - let Iceberg create it automatically on first write
    # with the correct S3 location. Pre-creating via SQL defaults to file:// location.

    # Write using foreachBatch (batch mode within streaming context)
    query = enriched_df.writeStream \
        .foreachBatch(write_batch_to_iceberg) \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze_transactions") \
        .trigger(processingTime='30 seconds') \
        .start()

    logger.info("Streaming query started - using foreachBatch to write to Iceberg")

    return query


def main():
    """Main execution function"""
    logger.info("Starting Kafka to Iceberg streaming job")

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Bronze database already created via Trino
    logger.info("Using existing bronze database")

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
