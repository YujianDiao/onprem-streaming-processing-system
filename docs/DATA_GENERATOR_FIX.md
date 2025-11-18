# Data Generator Topic Fix - Complete

## Issue
Data generator was sending transactions to `banking-transactions` instead of `banking-transactions-raw`.

## Root Cause
The `KAFKA_TOPIC` environment variable in `docker-compose.yml` was set to `banking-transactions` instead of `banking-transactions-raw`.

## Solution Applied

### Changed File: docker-compose.yml

**Before:**
```yaml
data-generator:
  environment:
    KAFKA_TOPIC: 'banking-transactions'
```

**After:**
```yaml
data-generator:
  environment:
    KAFKA_TOPIC: 'banking-transactions-raw'
```

### Actions Taken

1. ✅ Updated `docker-compose.yml` (line 191)
2. ✅ Stopped and removed data-generator container
3. ✅ Recreated data-generator with new configuration
4. ✅ Verified messages are flowing to `banking-transactions-raw`

---

## Verification

### Check Current Topic Configuration
```bash
docker exec data-generator python -c "import os; print('Topic:', os.getenv('KAFKA_TOPIC'))"
```

**Output:** `Topic: banking-transactions-raw` ✅

### Verify Messages in Topic
```bash
docker exec kafka-1 kafka-console-consumer \
  --topic banking-transactions-raw \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 3
```

**Result:** Successfully consuming transactions from `banking-transactions-raw` ✅

### Sample Transaction
```json
{
  "transaction_id": "44da7e3f-812c-405e-a40e-f78163bd7c92",
  "timestamp": "2025-11-16T15:42:04.692061Z",
  "account_id": "ACC8001",
  "transaction_type": "DEPOSIT",
  "amount": 2437.48,
  "currency": "USD",
  "merchant": null,
  "location": {
    "city": "Philadelphia",
    "state": "PA",
    "country": "US",
    "latitude": 29.201194,
    "longitude": -93.607478
  },
  "customer": {
    "customer_id": "CUST725",
    "name": "Jordan Marshall",
    "email": "uritter@example.net",
    "phone": "001-518-234-9328x23587"
  },
  "is_fraud": false,
  "status": "PENDING",
  "metadata": {
    "device_type": "atm",
    "ip_address": "89.210.70.91",
    "session_id": "5aa04371-4c2d-44f9-b179-2539f7f94d44"
  }
}
```

---

## Data Generator Details

### Current Configuration

| Setting | Value |
|---------|-------|
| **Kafka Bootstrap** | kafka-1:29092, kafka-2:29092, kafka-3:29092 |
| **Topic** | banking-transactions-raw ✅ |
| **Schema Registry** | http://schema-registry:8081 |
| **Generation Interval** | 2000ms (2 seconds per transaction) |

### Transaction Types Generated

| Type | Probability | Min Amount | Max Amount |
|------|-------------|------------|------------|
| PURCHASE | 45% | $10 | $500 |
| WITHDRAWAL | 20% | $20 | $1,000 |
| DEPOSIT | 15% | $50 | $5,000 |
| TRANSFER | 15% | $100 | $10,000 |
| PAYMENT | 5% | $500 | $50,000 |

### Fraud Injection
- **Fraud Rate:** 2% of transactions
- **Fraud Amounts:** $10,000 - $50,000
- **Fraud Flag:** `is_fraud: true`

---

## Topic Architecture

### Current Topics

```bash
docker exec kafka-1 kafka-topics --list --bootstrap-server localhost:9092 | grep banking
```

**Topics:**
- `banking-transactions` - Old topic (legacy)
- `banking-transactions-raw` - ✅ Current target (raw ingestion)
- `banking.transactions.r` - Intermediate topic
- `banking.accounts.cdc` - CDC stream
- `banking.customers.enriched` - Enriched customer data
- `banking.dlq` - Dead letter queue

### Data Flow

```
Data Generator
     ↓
banking-transactions-raw (✅ Current)
     ↓
Spark Streaming Job
     ↓
Iceberg Tables (Lakehouse)
     ↓
Trino SQL Analytics
```

---

## Monitoring Data Flow

### Check Message Count
```bash
docker exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic banking-transactions-raw
```

### View Latest Messages
```bash
docker exec kafka-1 kafka-console-consumer \
  --topic banking-transactions-raw \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

### Check Data Generator Logs
```bash
docker logs data-generator --tail 50
```

**Expected Output:**
```
Starting data generator...
Kafka Bootstrap Servers: ['kafka-1:29092', 'kafka-2:29092', 'kafka-3:29092']
Target Topic: banking-transactions-raw
Generation Interval: 2000ms
Connected to Kafka successfully!
Generated 10 transactions...
Generated 20 transactions...
```

---

## Restart Data Generator (if needed)

### Method 1: Simple Restart
```bash
docker compose restart data-generator
```

### Method 2: Full Recreate (applies config changes)
```bash
docker compose stop data-generator
docker compose rm -f data-generator
docker compose up -d data-generator
```

### Method 3: Rebuild (if code changes)
```bash
docker compose up -d --build data-generator
```

---

## Troubleshooting

### No Messages in Topic

**Check if generator is running:**
```bash
docker compose ps data-generator
```

**Check for errors:**
```bash
docker logs data-generator
```

**Check environment variables:**
```bash
docker exec data-generator env | grep KAFKA
```

### Wrong Topic

**Verify topic setting:**
```bash
docker exec data-generator python -c "import os; print(os.getenv('KAFKA_TOPIC'))"
```

**If wrong, update docker-compose.yml and recreate:**
```bash
# Edit docker-compose.yml
docker compose stop data-generator
docker compose rm -f data-generator
docker compose up -d data-generator
```

### Generator Not Producing

**Check Kafka connectivity:**
```bash
docker exec data-generator python -c "from kafka import KafkaProducer; print('OK')"
```

**Check Schema Registry:**
```bash
docker exec data-generator curl -s http://schema-registry:8081/subjects | jq
```

---

## Summary

✅ **Issue:** Data generator sending to wrong topic
✅ **Fix Applied:** Changed `KAFKA_TOPIC` from `banking-transactions` to `banking-transactions-raw`
✅ **Verified:** Messages successfully flowing to correct topic
✅ **Container:** Recreated with new configuration
✅ **Status:** WORKING

**Current State:**
- Data generator: ✅ Running
- Target topic: ✅ `banking-transactions-raw`
- Message flow: ✅ Confirmed
- Generation rate: ✅ 1 transaction per 2 seconds

The data pipeline is now correctly configured for the lakehouse architecture!
