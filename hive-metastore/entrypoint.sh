#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h postgres -p 5432 -U hive; do
  echo "Waiting for postgres..."
  sleep 2
done

echo "PostgreSQL is ready!"

# Set environment variables for schematool (database connection only)
export HADOOP_CLIENT_OPTS="-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver \
  -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres:5432/metastore \
  -Djavax.jdo.option.ConnectionUserName=hive \
  -Djavax.jdo.option.ConnectionPassword=hive123"

# Check if schema needs initialization
echo "Checking Hive Metastore schema..."
if ! /opt/hive/bin/schematool -dbType postgres -info 2>/dev/null; then
  echo "Initializing Hive Metastore schema..."
  /opt/hive/bin/schematool -dbType postgres -initSchema
  echo "Schema initialized successfully!"
else
  echo "Hive Metastore schema already exists."
fi

# Configure S3/MinIO credentials for Hive Metastore service
# These will be used when metastore validates S3 paths during table creation
export HADOOP_OPTS="${HADOOP_OPTS} -Dfs.s3a.endpoint=http://minio:9000 \
  -Dfs.s3a.access.key=minioadmin \
  -Dfs.s3a.secret.key=minioadmin \
  -Dfs.s3a.path.style.access=true \
  -Dfs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  -Dfs.s3a.connection.ssl.enabled=false"

echo "S3 Configuration applied to Hive Metastore"
echo "HADOOP_OPTS: ${HADOOP_OPTS}"

# Start Hive Metastore
echo "Starting Hive Metastore..."
exec /opt/hive/bin/hive --service metastore
