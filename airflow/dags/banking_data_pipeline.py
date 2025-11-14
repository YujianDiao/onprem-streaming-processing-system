"""
Banking Data Pipeline DAG
This DAG orchestrates a complete data pipeline:
1. Triggers Spark streaming job
2. Runs batch aggregations
3. Loads data to PostgreSQL serving layer
4. Performs data quality checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# Define the DAG
dag = DAG(
    'banking_data_pipeline',
    default_args=default_args,
    description='End-to-end banking data processing pipeline',
    schedule_interval='@hourly',
    catchup=False,
    tags=['banking', 'data-pipeline', 'production'],
)


def check_kafka_topics(**context):
    """Check if required Kafka topics exist and have data"""
    from kafka import KafkaConsumer
    import logging

    logging.info("Checking Kafka topics...")
    topics = ['banking.transactions.raw', 'banking.accounts.cdc']

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=['kafka-1:29092', 'kafka-2:29093', 'kafka-3:29094'],
            consumer_timeout_ms=1000
        )
        available_topics = consumer.topics()

        for topic in topics:
            if topic not in available_topics:
                raise ValueError(f"Topic {topic} does not exist")
            logging.info(f"Topic {topic} is available")

        consumer.close()
        return True
    except Exception as e:
        logging.error(f"Kafka check failed: {str(e)}")
        raise


def validate_minio_connection(**context):
    """Validate MinIO connection and required buckets"""
    import boto3
    from botocore.client import Config
    import logging

    logging.info("Validating MinIO connection...")

    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4')
    )

    required_buckets = ['lakehouse', 'raw-data', 'warehouse']
    existing_buckets = [bucket['Name'] for bucket in s3_client.list_buckets()['Buckets']]

    for bucket in required_buckets:
        if bucket not in existing_buckets:
            raise ValueError(f"Bucket {bucket} does not exist in MinIO")
        logging.info(f"Bucket {bucket} is available")

    return True


def run_data_quality_checks(**context):
    """Run data quality checks on processed data"""
    import logging

    logging.info("Running data quality checks...")

    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # Check 1: Row count validation
    row_count_query = """
        SELECT COUNT(*) as cnt
        FROM serving.daily_transactions
        WHERE transaction_date = CURRENT_DATE - INTERVAL '1 day'
    """
    result = pg_hook.get_first(row_count_query)
    row_count = result[0] if result else 0

    logging.info(f"Row count for yesterday: {row_count}")

    if row_count == 0:
        logging.warning("No data found for yesterday - this may be expected on first run")

    # Check 2: Null value checks
    null_check_query = """
        SELECT
            COUNT(*) as total_rows,
            SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) as null_transaction_ids,
            SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amounts
        FROM serving.daily_transactions
        WHERE transaction_date = CURRENT_DATE - INTERVAL '1 day'
    """
    result = pg_hook.get_first(null_check_query)

    if result and (result[1] > 0 or result[2] > 0):
        raise ValueError(f"Data quality check failed: Found null values - {result}")

    logging.info("Data quality checks passed!")
    return True


# Task 1: Check Kafka topics
check_kafka = PythonOperator(
    task_id='check_kafka_topics',
    python_callable=check_kafka_topics,
    dag=dag,
)

# Task 2: Validate MinIO connection
validate_minio = PythonOperator(
    task_id='validate_minio_connection',
    python_callable=validate_minio_connection,
    dag=dag,
)

# Task 3: Spark Streaming Job (reads from Kafka, writes to Iceberg)
spark_streaming_job = SparkSubmitOperator(
    task_id='spark_streaming_to_iceberg',
    application='/opt/bitnami/spark/jobs/kafka_to_iceberg_streaming.py',
    conn_id='spark_default',
    total_executor_cores=2,
    executor_memory='2g',
    driver_memory='1g',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.local': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.local.type': 'hadoop',
        'spark.sql.catalog.local.warehouse': 's3a://lakehouse/',
    },
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4',
    dag=dag,
)

# Task 4: Batch aggregation job
spark_batch_aggregation = SparkSubmitOperator(
    task_id='spark_batch_aggregation',
    application='/opt/bitnami/spark/jobs/batch_aggregations.py',
    conn_id='spark_default',
    total_executor_cores=2,
    executor_memory='2g',
    driver_memory='1g',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    },
    packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4',
    dag=dag,
)

# Task 5: Create serving tables in PostgreSQL
create_serving_tables = PostgresOperator(
    task_id='create_serving_tables',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS serving.daily_transactions (
            transaction_id VARCHAR(100) PRIMARY KEY,
            account_id VARCHAR(100),
            transaction_type VARCHAR(50),
            amount DECIMAL(18,2),
            currency VARCHAR(10),
            transaction_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_daily_transactions_date
        ON serving.daily_transactions(transaction_date);

        CREATE INDEX IF NOT EXISTS idx_daily_transactions_account
        ON serving.daily_transactions(account_id);
    """,
    dag=dag,
)

# Task 6: Load aggregated data to PostgreSQL
load_to_postgres = SparkSubmitOperator(
    task_id='load_aggregated_to_postgres',
    application='/opt/bitnami/spark/jobs/iceberg_to_postgres.py',
    conn_id='spark_default',
    total_executor_cores=1,
    executor_memory='2g',
    driver_memory='1g',
    packages='org.postgresql:postgresql:42.6.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2',
    dag=dag,
)

# Task 7: Data quality checks
data_quality_checks = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

# Define task dependencies
check_kafka >> validate_minio >> spark_streaming_job
spark_streaming_job >> spark_batch_aggregation
spark_batch_aggregation >> create_serving_tables >> load_to_postgres
load_to_postgres >> data_quality_checks
