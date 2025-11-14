#!/bin/bash
# Script to configure Kafka Datagen connector for banking data

set -e

CONNECT_URL="http://localhost:8083"

echo "Waiting for Kafka Connect to be ready..."
until curl -s -f -o /dev/null "$CONNECT_URL"; do
    echo "Waiting for Kafka Connect..."
    sleep 5
done

echo "Configuring Datagen connector for banking transactions..."

curl -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d '{
        "name": "datagen-banking-transactions",
        "config": {
            "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
            "kafka.topic": "banking.transactions.raw",
            "quickstart": "transactions",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "max.interval": 1000,
            "iterations": -1,
            "tasks.max": "1",
            "schema.string": "{\"type\":\"record\",\"name\":\"Transaction\",\"fields\":[{\"name\":\"transaction_id\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"regex\":\"TXN[0-9]{10}\"}}},{\"name\":\"account_id\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"regex\":\"ACC[0-9]{8}\"}}},{\"name\":\"transaction_type\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"DEBIT\",\"CREDIT\"]}}},{\"name\":\"amount\",\"type\":{\"type\":\"double\",\"arg.properties\":{\"range\":{\"min\":1.0,\"max\":5000.0}}}},{\"name\":\"currency\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"USD\",\"EUR\",\"GBP\"]}}},{\"name\":\"merchant\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Amazon\",\"Walmart\",\"Target\",\"Starbucks\",\"Shell\",\"McDonalds\",\"Apple Store\",\"Best Buy\"]}}},{\"name\":\"merchant_category\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Retail\",\"Food\",\"Gas\",\"Technology\",\"Entertainment\"]}}},{\"name\":\"transaction_timestamp\",\"type\":{\"type\":\"long\",\"format_as_time\":\"unix\",\"arg.properties\":{\"iteration\":{\"start\":1,\"step\":10}}}},{\"name\":\"status\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"COMPLETED\",\"PENDING\",\"FAILED\"]}}}]}"
        }
    }'

echo ""
echo "Datagen connector configured successfully!"
echo "Checking connector status..."

curl -s "$CONNECT_URL/connectors/datagen-banking-transactions/status" | jq .

echo ""
echo "Banking transaction data is now being generated to topic: banking.transactions.raw"
