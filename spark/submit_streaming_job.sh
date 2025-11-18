#!/bin/bash

echo "============================================"
echo "Submitting Kafka to Iceberg Streaming Job"
echo "============================================"

/opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg.type=hive \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    --conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse/ \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark/jobs/kafka_to_iceberg_streaming.py

echo "============================================"
echo "Job submission complete"
echo "============================================"
