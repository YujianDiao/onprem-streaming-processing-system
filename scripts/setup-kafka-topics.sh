#!/bin/bash
# Script to create Kafka topics for the banking data pipeline

set -e

KAFKA_BROKER="kafka-1:29092"
REPLICATION_FACTOR=2
PARTITIONS=3

echo "Waiting for Kafka to be ready..."
sleep 30

echo "Creating Kafka topics..."

# Banking transactions topic
docker exec kafka-1 kafka-topics --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $REPLICATION_FACTOR \
    --partitions $PARTITIONS \
    --topic banking.transactions.raw \
    --if-not-exists \
    --config retention.ms=604800000 \
    --config compression.type=snappy

# Account CDC topic
docker exec kafka-1 kafka-topics --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $REPLICATION_FACTOR \
    --partitions $PARTITIONS \
    --topic banking.accounts.cdc \
    --if-not-exists \
    --config retention.ms=604800000 \
    --config cleanup.policy=compact

# Customer enriched topic
docker exec kafka-1 kafka-topics --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $REPLICATION_FACTOR \
    --partitions $PARTITIONS \
    --topic banking.customers.enriched \
    --if-not-exists \
    --config retention.ms=604800000

# Dead letter queue
docker exec kafka-1 kafka-topics --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $REPLICATION_FACTOR \
    --partitions $PARTITIONS \
    --topic banking.dlq \
    --if-not-exists \
    --config retention.ms=2592000000

echo "Listing all topics:"
docker exec kafka-1 kafka-topics --list --bootstrap-server $KAFKA_BROKER

echo "Kafka topics created successfully!"
