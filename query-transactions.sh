#!/bin/bash
# Query Iceberg transactions table using Spark SQL

echo "============================================"
echo "Querying Iceberg Bronze Transactions Table"
echo "============================================"
echo ""

docker exec spark-master /opt/spark/bin/spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hive \
  --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.iceberg.warehouse=s3a://lakehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  -e "
-- Count total records
SELECT COUNT(*) as total_records
FROM iceberg.bronze.transactions;

-- Show sample transactions
SELECT
  transaction_id,
  timestamp,
  account_id,
  transaction_type,
  amount,
  currency,
  is_fraud,
  status
FROM iceberg.bronze.transactions
LIMIT 10;

-- Summary by transaction type
SELECT
  transaction_type,
  COUNT(*) as count,
  ROUND(AVG(amount), 2) as avg_amount,
  ROUND(MIN(amount), 2) as min_amount,
  ROUND(MAX(amount), 2) as max_amount
FROM iceberg.bronze.transactions
GROUP BY transaction_type
ORDER BY count DESC;

-- Fraud detection summary
SELECT
  is_fraud,
  COUNT(*) as count,
  ROUND(AVG(amount), 2) as avg_amount
FROM iceberg.bronze.transactions
GROUP BY is_fraud;
"

echo ""
echo "============================================"
echo "Query Complete"
echo "============================================"
