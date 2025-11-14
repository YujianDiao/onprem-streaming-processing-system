"""
Iceberg to PostgreSQL Job
Loads aggregated data from Gold layer Iceberg tables to PostgreSQL serving layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session with PostgreSQL and Iceberg configurations"""
    return SparkSession.builder \
        .appName("IcebergToPostgreSQL") \
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
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()


def get_postgres_properties():
    """Get PostgreSQL connection properties"""
    return {
        "user": "admin",
        "password": "admin123",
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified"
    }


def load_daily_transactions(spark, processing_date=None):
    """
    Load daily transaction details from Silver layer to PostgreSQL
    """
    logger.info("Loading daily transactions to PostgreSQL")

    # Read from Silver layer
    transactions_df = spark.table("local.silver.transactions")

    # Filter for specific date
    if processing_date:
        transactions_df = transactions_df.filter(col("date_partition") == processing_date)
    else:
        transactions_df = transactions_df.filter(
            col("date_partition") == date_sub(current_date(), 1)
        )

    # Select and prepare columns for PostgreSQL
    postgres_df = transactions_df.select(
        col("transaction_id"),
        col("account_id"),
        col("transaction_type"),
        col("amount"),
        col("currency"),
        to_date(col("transaction_timestamp")).alias("transaction_date"),
        col("processing_timestamp").alias("created_at")
    )

    # Write to PostgreSQL
    postgres_url = "jdbc:postgresql://postgres:5432/metastore"
    postgres_properties = get_postgres_properties()

    logger.info(f"Writing {postgres_df.count()} transactions to PostgreSQL")

    postgres_df.write \
        .jdbc(
            url=postgres_url,
            table="serving.daily_transactions",
            mode="append",
            properties=postgres_properties
        )

    logger.info("Daily transactions loaded successfully")


def load_account_summaries(spark, processing_date=None):
    """
    Load account summaries from Gold layer to PostgreSQL
    """
    logger.info("Loading account summaries to PostgreSQL")

    # Read from Gold layer
    summaries_df = spark.table("local.gold.daily_account_summary")

    # Filter for specific date
    if processing_date:
        summaries_df = summaries_df.filter(col("transaction_date") == processing_date)
    else:
        summaries_df = summaries_df.filter(
            col("transaction_date") == date_sub(current_date(), 1)
        )

    # Write to PostgreSQL
    postgres_url = "jdbc:postgresql://postgres:5432/metastore"
    postgres_properties = get_postgres_properties()

    # Create table if not exists
    logger.info("Ensuring account_summaries table exists")

    spark.read \
        .jdbc(
            url=postgres_url,
            table="(SELECT 1) as dummy",
            properties=postgres_properties
        ) \
        .limit(0)

    logger.info(f"Writing {summaries_df.count()} account summaries to PostgreSQL")

    summaries_df.write \
        .jdbc(
            url=postgres_url,
            table="serving.account_summaries",
            mode="append",
            properties=postgres_properties
        )

    logger.info("Account summaries loaded successfully")


def load_merchant_analytics(spark, processing_date=None):
    """
    Load merchant analytics from Gold layer to PostgreSQL
    """
    logger.info("Loading merchant analytics to PostgreSQL")

    # Read from Gold layer
    merchant_df = spark.table("local.gold.merchant_summary")

    # Filter for specific date
    if processing_date:
        merchant_df = merchant_df.filter(col("transaction_date") == processing_date)
    else:
        merchant_df = merchant_df.filter(
            col("transaction_date") == date_sub(current_date(), 1)
        )

    # Write to PostgreSQL
    postgres_url = "jdbc:postgresql://postgres:5432/metastore"
    postgres_properties = get_postgres_properties()

    logger.info(f"Writing {merchant_df.count()} merchant analytics to PostgreSQL")

    merchant_df.write \
        .jdbc(
            url=postgres_url,
            table="serving.merchant_analytics",
            mode="append",
            properties=postgres_properties
        )

    logger.info("Merchant analytics loaded successfully")


def create_materialized_views(spark):
    """
    Create materialized views in PostgreSQL for common queries
    """
    logger.info("Creating materialized views in PostgreSQL")

    postgres_url = "jdbc:postgresql://postgres:5432/metastore"
    postgres_properties = get_postgres_properties()

    # Get a connection to execute DDL
    from pyspark.sql import DataFrameWriter
    import psycopg2

    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="metastore",
            user="admin",
            password="admin123"
        )
        cursor = conn.cursor()

        # Create materialized view for top merchants
        cursor.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS serving.top_merchants_by_volume AS
            SELECT
                merchant,
                merchant_category,
                SUM(total_amount) as total_volume,
                SUM(total_transactions) as transaction_count,
                AVG(avg_amount) as avg_transaction_amount
            FROM serving.merchant_analytics
            WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY merchant, merchant_category
            ORDER BY total_volume DESC
            LIMIT 100
        """)

        # Create materialized view for account trends
        cursor.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS serving.account_trends AS
            SELECT
                account_id,
                transaction_date,
                total_transactions,
                net_amount,
                LAG(net_amount) OVER (PARTITION BY account_id ORDER BY transaction_date) as prev_day_net,
                net_amount - LAG(net_amount) OVER (PARTITION BY account_id ORDER BY transaction_date) as daily_change
            FROM serving.account_summaries
            ORDER BY account_id, transaction_date DESC
        """)

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Materialized views created successfully")

    except Exception as e:
        logger.error(f"Error creating materialized views: {str(e)}")
        raise


def refresh_materialized_views():
    """
    Refresh materialized views in PostgreSQL
    """
    logger.info("Refreshing materialized views")

    import psycopg2

    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="metastore",
            user="admin",
            password="admin123"
        )
        cursor = conn.cursor()

        cursor.execute("REFRESH MATERIALIZED VIEW serving.top_merchants_by_volume")
        cursor.execute("REFRESH MATERIALIZED VIEW serving.account_trends")

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Materialized views refreshed successfully")

    except Exception as e:
        logger.error(f"Error refreshing materialized views: {str(e)}")
        # Don't raise - views might not exist on first run


def main():
    """Main execution function"""
    logger.info("Starting Iceberg to PostgreSQL job")

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Load data to PostgreSQL
        load_daily_transactions(spark)
        load_account_summaries(spark)
        load_merchant_analytics(spark)

        # Create and refresh materialized views
        create_materialized_views(spark)
        refresh_materialized_views()

        logger.info("Data loading to PostgreSQL completed successfully")

    except Exception as e:
        logger.error(f"Error in Iceberg to PostgreSQL job: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
