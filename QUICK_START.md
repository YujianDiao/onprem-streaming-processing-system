# Quick Start Guide

Get started with the data lakehouse platform in minutes!

## Prerequisites Check

```bash
# Check Docker
docker --version
# Should be 20.10+

# Check Docker Compose
docker-compose --version
# Should be 2.0+

# Check available resources
docker info | grep -E "CPUs|Total Memory"
# Minimum: 8 CPUs, 12GB RAM
# Recommended: 16 CPUs, 24GB RAM
```

## Installation

### Standard Setup (24GB+ RAM)

```bash
# 1. Clone and navigate
git clone <repo-url>
cd onprem-streaming-processing-system

# 2. Quick setup
make setup

# 3. Start everything
make start

# Wait 5-10 minutes for all services to initialize
```

### Lightweight Setup (12-16GB RAM)

```bash
# 1. Clone and navigate
git clone <repo-url>
cd onprem-streaming-processing-system

# 2. Setup
make setup

# 3. Start with lightweight config
docker-compose -f docker-compose.yml -f docker-compose.lightweight.yml up -d

# Wait for services to initialize
```

## Verify Installation

```bash
# Check all services are running
make status

# Test connectivity
make test-all

# View service URLs
make urls
```

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | None |
| Trino | http://localhost:8086 | None |
| Spark Master | http://localhost:8888 | None |
| Grafana | http://localhost:3000 | admin/admin |
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin |
| Prometheus | http://localhost:9090 | None |

Quick access:
```bash
make all-ui  # Opens all UIs in browser
```

## Start Generating Data

The platform includes a Python-based data generator that creates realistic banking transactions:

```bash
# Start data generator
make datagen-start

# View generator logs
make logs-datagen

# Stop generator
make datagen-stop
```

The generator will continuously produce transactions to the `banking-transactions` Kafka topic.

## View Streaming Data

### Check Kafka Messages

```bash
# Via Kafka UI (Recommended)
open http://localhost:8080
# Navigate to Topics → banking-transactions → Messages

# Via CLI
docker exec kafka-1 kafka-console-consumer \
  --topic banking-transactions \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

### Monitor Spark Streaming

Open http://localhost:8888 to see:
- Running streaming jobs
- Executor status
- Job progress and stages
- Event timeline

## Query the Data Lakehouse with Trino

### Connect to Trino CLI

```bash
make shell-trino
# Or: docker exec -it trino trino
```

### Basic Queries

```sql
-- Show available catalogs
SHOW CATALOGS;

-- Show schemas in Iceberg catalog
SHOW SCHEMAS IN iceberg;

-- List tables (after Spark has written data)
SHOW TABLES IN iceberg.default;

-- Query transaction data
SELECT * FROM iceberg.default.transactions LIMIT 10;

-- Count transactions
SELECT COUNT(*) as total_transactions
FROM iceberg.default.transactions;

-- Aggregate by transaction type
SELECT
    transaction_type,
    COUNT(*) as count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM iceberg.default.transactions
GROUP BY transaction_type
ORDER BY total_amount DESC;

-- Find fraudulent transactions
SELECT
    customer_id,
    transaction_id,
    amount,
    merchant,
    timestamp
FROM iceberg.default.transactions
WHERE is_fraud = true
ORDER BY timestamp DESC
LIMIT 20;
```

### Time Travel Queries (Iceberg Feature)

```sql
-- View table snapshots
SELECT * FROM iceberg.default.transactions$snapshots;

-- Query historical data
SELECT * FROM iceberg.default.transactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 12:00:00'
LIMIT 10;

-- Show table files
SELECT * FROM iceberg.default.transactions$files;
```

## Architecture at a Glance

```
┌──────────────────┐
│ Data Generator   │
│   (Python)       │
└────────┬─────────┘
         │
         ▼
  ┌─────────────┐      ┌──────────────┐      ┌──────────────┐
  │   Kafka     │─────▶│    Spark     │─────▶│   Iceberg    │
  │ (3 brokers) │      │  (Streaming) │      │   (MinIO)    │
  │  KRaft Mode │      └──────────────┘      └──────┬───────┘
  └─────────────┘                                    │
                                                     │
                    ┌────────────────────────────────┘
                    │
                    ▼
            ┌───────────────┐
            │ Hive Metastore│
            │  (PostgreSQL) │
            └───────┬───────┘
                    │
                    ▼
            ┌───────────────┐
            │     Trino     │
            │ (SQL Analytics)│
            └───────────────┘
                    │
                    ▼
            ┌───────────────┐
            │   Grafana     │
            │  (Monitoring) │
            └───────────────┘
```

## Data Flow

1. **Data Generator** creates synthetic banking transactions (JSON)
2. **Kafka** receives and distributes events across 3 brokers
3. **Spark Streaming** reads from Kafka, writes to Iceberg tables in MinIO
4. **Hive Metastore** stores table metadata (schemas, partitions, locations)
5. **Trino** queries Iceberg tables directly using SQL
6. **Grafana** monitors system health and performance metrics

**Key Point**: No orchestration or serving layer needed! Trino queries the lakehouse directly.

## Sample Queries by Use Case

### Real-Time Analytics

```sql
-- Trino CLI
-- Transactions in last hour
SELECT COUNT(*), SUM(amount)
FROM iceberg.default.transactions
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- High-value transactions
SELECT * FROM iceberg.default.transactions
WHERE amount > 10000
ORDER BY timestamp DESC
LIMIT 20;
```

### Business Intelligence

```sql
-- Top merchants by revenue
SELECT
    merchant,
    COUNT(*) as transaction_count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_transaction
FROM iceberg.default.transactions
WHERE merchant IS NOT NULL
GROUP BY merchant
ORDER BY total_revenue DESC
LIMIT 10;

-- Customer transaction patterns
SELECT
    customer_id,
    COUNT(*) as txn_count,
    SUM(amount) as total_spent,
    COUNT(DISTINCT merchant) as unique_merchants
FROM iceberg.default.transactions
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;
```

### Fraud Detection

```sql
-- Fraud rate by transaction type
SELECT
    transaction_type,
    COUNT(*) as total,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
    CAST(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100 as fraud_rate
FROM iceberg.default.transactions
GROUP BY transaction_type
ORDER BY fraud_rate DESC;
```

### Metadata Exploration

```sql
-- Check data freshness
SELECT
    MAX(timestamp) as latest_transaction,
    CURRENT_TIMESTAMP as current_time,
    CURRENT_TIMESTAMP - MAX(timestamp) as data_lag
FROM iceberg.default.transactions;

-- Table statistics
SELECT
    file_path,
    file_size_in_bytes / 1024 / 1024 as size_mb,
    record_count
FROM iceberg.default.transactions$files
ORDER BY record_count DESC;
```

## Common Commands

```bash
# Start services
make start              # All services
make start-core         # Kafka, PostgreSQL, MinIO, Hive Metastore
make start-processing   # Spark cluster
make start-analytics    # Trino
make start-monitoring   # Prometheus, Grafana

# Data generation
make datagen-start      # Start data generator
make datagen-stop       # Stop data generator
make logs-datagen       # View generator logs

# Monitor
make logs               # All logs
make logs-kafka         # Kafka logs
make logs-spark         # Spark logs
make logs-trino         # Trino logs

# Management
make stop               # Stop all services
make restart            # Restart all
make status             # Service status
make health             # Health check

# Testing
make test-all           # Test all services
make test-kafka         # Test Kafka
make test-postgres      # Test PostgreSQL (Hive Metastore)
make test-trino         # Test Trino

# Utilities
make shell-kafka        # Kafka shell
make shell-postgres     # PostgreSQL shell (Hive Metastore)
make shell-spark        # Spark shell
make shell-trino        # Trino CLI
make backup-postgres    # Backup Hive Metastore
make backup-minio       # Backup Iceberg data
```

## Monitoring & Observability

### Kafka UI (http://localhost:8080)

- View topics and partitions
- Browse messages in real-time
- Monitor consumer lag
- Check broker health
- View schema registry

### Spark UI (http://localhost:8888)

- Running/completed applications
- Job stages and tasks
- Executor status and metrics
- Event timeline
- Storage tab for Iceberg

### Trino UI (http://localhost:8086)

- Running queries
- Query history
- Cluster status
- Worker nodes
- Query plans

### Grafana (http://localhost:3000)

1. Login: admin/admin
2. Add Prometheus data source: http://prometheus:9090
3. Import dashboards for:
   - Kafka JMX metrics
   - Spark metrics
   - System resources
   - MinIO metrics

## Troubleshooting

### Services won't start

```bash
# Check Docker resources
docker info | grep -E "CPUs|Total Memory"

# Check logs
make logs

# Restart services
make restart
```

### Out of memory

```bash
# Use lightweight setup
docker-compose -f docker-compose.yml -f docker-compose.lightweight.yml restart

# Or stop non-essential services
docker-compose stop datahub-mysql datahub-elasticsearch datahub-gms datahub-frontend

# Check resource usage
docker stats
```

### Kafka brokers not connecting

```bash
# Check Kafka logs (KRaft mode - no Zookeeper)
make logs-kafka

# Verify Kafka cluster
make test-kafka

# Restart Kafka cluster
docker-compose restart kafka-1 kafka-2 kafka-3
```

### Data generator not producing

```bash
# Check generator status
docker-compose ps data-generator

# View logs
make logs-datagen

# Restart generator
make datagen-stop
make datagen-start
```

### No data in Trino

```bash
# 1. Check if data generator is running
docker-compose ps data-generator

# 2. Verify Kafka has messages
docker exec kafka-1 kafka-console-consumer \
  --topic banking-transactions \
  --bootstrap-server localhost:9092 \
  --max-messages 5

# 3. Check Spark is processing
make logs-spark | grep -i streaming

# 4. Verify Hive Metastore connection
docker-compose logs hive-metastore

# 5. Check if tables exist in Trino
docker exec trino trino --execute "SHOW TABLES IN iceberg.default"
```

### Trino queries failing

```bash
# Check Trino logs
make logs-trino

# Verify Hive Metastore
docker-compose logs hive-metastore

# Test Trino connection
make test-trino

# Check catalog configuration
docker exec trino cat /etc/trino/catalog/iceberg.properties
```

## Cleanup

```bash
# Stop services (keep data)
make stop

# Stop and remove volumes (delete all data)
make clean

# Complete cleanup (delete everything)
make clean-all
```

## Next Steps

1. ✅ **Monitor Data Flow**: Watch Kafka UI for incoming transactions
2. ✅ **Run SQL Queries**: Use Trino to analyze data in the lakehouse
3. ✅ **Explore Iceberg Features**: Try time travel and metadata queries
4. ✅ **Create Dashboards**: Build Grafana dashboards for metrics
5. ✅ **Customize Generator**: Modify data generation patterns
6. ✅ **Optimize Performance**: Tune Spark, Kafka, and Trino configurations

## What Makes This Different?

This is a **data lakehouse** platform, not a traditional data lake or warehouse:

✅ **Query data lake directly** - No ETL to warehouse needed
✅ **ACID transactions** - Reliable updates/deletes with Iceberg
✅ **Time travel** - Query historical snapshots
✅ **Schema evolution** - Add/modify columns without downtime
✅ **Open formats** - Parquet + Iceberg = no vendor lock-in
✅ **Unified architecture** - One platform for batch and streaming

## Getting Help

- Full documentation: [README.md](README.md)
- Architecture details: [ARCHITECTURE.md](ARCHITECTURE.md)
- Makefile commands: `make help`
- Logs: `make logs-[service-name]`

## Useful Resources

- Apache Kafka: https://kafka.apache.org/documentation/
- Apache Spark: https://spark.apache.org/docs/latest/
- Apache Iceberg: https://iceberg.apache.org/docs/latest/
- Trino: https://trino.io/docs/current/
- Hive Metastore: https://cwiki.apache.org/confluence/display/Hive/Design

---

**Ready to explore the data lakehouse? Start querying!** 🚀
