"""
Test Kafka Read - Simple job to verify Kafka connectivity
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Kafka read test")

    spark = SparkSession.builder \
        .appName("KafkaReadTest") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    logger.info("Connecting to Kafka...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-1:29092,kafka-2:29092,kafka-3:29092") \
        .option("subscribe", "banking.transactions.raw") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info("Connected to Kafka!")

    # Just extract the value as string
    messages_df = kafka_df.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
    )

    # Write to console to see if we're getting data
    query = messages_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .option("numRows", "5") \
        .trigger(processingTime='10 seconds') \
        .start()

    logger.info("Console streaming query started")
    query.awaitTermination()

if __name__ == "__main__":
    main()
