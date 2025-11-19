#!/bin/bash

# ============================================================================
# Setup Kafka Topics for Flink Jobs
# ============================================================================
# This script creates all necessary Kafka topics for Flink streaming jobs
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Setting up Kafka topics for Flink jobs...${NC}"

# Kafka broker
KAFKA_BROKER="kafka-1:29092"

# Function to create topic
create_topic() {
    local topic=$1
    local partitions=${2:-6}
    local replication=${3:-2}

    echo -e "${YELLOW}Creating topic: $topic (partitions=$partitions, replication=$replication)${NC}"

    docker exec kafka-1 kafka-topics --create \
        --bootstrap-server ${KAFKA_BROKER} \
        --topic ${topic} \
        --partitions ${partitions} \
        --replication-factor ${replication} \
        --if-not-exists \
        --config retention.ms=604800000 \
        --config segment.ms=86400000
}

# Create topics for Flink job outputs

# Fraud Detection outputs
create_topic "banking.fraud.alerts" 6 2

# Transaction Monitoring outputs
create_topic "banking.monitoring.alerts" 6 2
create_topic "banking.metrics.realtime" 6 2

# Balance Aggregation outputs
create_topic "banking.balances.realtime" 6 2
create_topic "banking.balance.alerts" 6 2
create_topic "banking.balance.snapshots" 3 2

echo ""
echo -e "${GREEN}✓ All Flink topics created successfully${NC}"
echo ""
echo "Topics created:"
echo "  - banking.fraud.alerts (fraud detection alerts)"
echo "  - banking.monitoring.alerts (transaction monitoring alerts)"
echo "  - banking.metrics.realtime (real-time banking metrics)"
echo "  - banking.balances.realtime (real-time account balances)"
echo "  - banking.balance.alerts (balance alerts)"
echo "  - banking.balance.snapshots (hourly balance snapshots)"
echo ""

# List all topics
echo -e "${YELLOW}All Kafka topics:${NC}"
docker exec kafka-1 kafka-topics --list --bootstrap-server ${KAFKA_BROKER}
