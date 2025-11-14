#!/bin/bash
set -e

# Function to create database if it doesn't exist
create_database() {
    local database=$1
    echo "Creating database '$database'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        SELECT 'CREATE DATABASE $database'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$database')\gexec
EOSQL
}

# Create multiple databases
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Creating multiple databases: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_database $db
    done
    echo "Multiple databases created"
fi

# Create schemas in the main database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS serving;
    CREATE SCHEMA IF NOT EXISTS analytics;
    CREATE SCHEMA IF NOT EXISTS staging;

    GRANT ALL PRIVILEGES ON SCHEMA serving TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON SCHEMA analytics TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON SCHEMA staging TO $POSTGRES_USER;
EOSQL

echo "PostgreSQL initialization completed"
