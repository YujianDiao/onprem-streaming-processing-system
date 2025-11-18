"""
Test Kafka Batch Read - Verify we can read from Kafka in batch mode
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Kafka batch read test")

    spark = SparkSession.builder \
        .appName("KafkaBatchReadTest") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka in BATCH mode
    logger.info("Reading from Kafka in batch mode...")
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29092,kafka-3:29092") \
        .option("subscribe", "banking.transactions.raw") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    logger.info("Successfully read from Kafka!")

    # Extract and show some data
    messages_df = kafka_df.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
    )

    count = messages_df.count()
    logger.info(f"Total messages read: {count}")

    logger.info("Sample messages:")
    messages_df.show(5, truncate=False)

    logger.info("Test completed successfully!")

if __name__ == "__main__":
    main()
