# Quick Start Guide

Get started with the data platform in minutes!

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
# Minimum: 8 CPUs, 16GB RAM
```

## Installation

### Standard Setup (32GB+ RAM)

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

### Lightweight Setup (16GB RAM)

```bash
# 1. Clone and navigate
git clone <repo-url>
cd onprem-streaming-processing-system

# 2. Setup
make setup

# 3. Start with lightweight config
docker-compose -f docker-compose.yml -f docker-compose.lightweight.yml up -d

# 4. Setup Kafka topics and data generation
make topics
sleep 10
make datagen
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
| Airflow | http://localhost:8085 | admin/admin |
| Spark Master | http://localhost:8888 | None |
| Superset | http://localhost:8088 | admin/admin |
| Grafana | http://localhost:3000 | admin/admin |
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin |

Quick access:
```bash
make all-ui  # Opens all UIs in browser
```

## Run Sample Pipeline

1. **Open Airflow**: http://localhost:8085
2. **Login**: admin/admin
3. **Enable DAG**: Toggle `banking_data_pipeline` to ON
4. **Trigger**: Click the play button
5. **Monitor**: Watch execution in Graph view

Expected duration: 5-10 minutes

## View Results

### Check Kafka Messages

```bash
# Via Kafka UI
Open http://localhost:8080

# Via CLI
docker exec kafka-1 kafka-console-consumer \
  --topic banking.transactions.raw \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

### Query PostgreSQL

```bash
make shell-postgres

# Inside PostgreSQL:
SELECT COUNT(*) FROM serving.daily_transactions;
SELECT * FROM serving.account_summaries LIMIT 10;
```

### Check Spark Jobs

Open http://localhost:8888 to see running/completed applications

### View in Superset

1. Open http://localhost:8088
2. Login: admin/admin
3. Data → Databases → Add Database
4. Connection: `postgresql://admin:admin123@postgres:5432/metastore`
5. Create charts from `serving` schema tables

## Common Commands

```bash
# Start services
make start              # All services
make start-core         # Only Kafka, PostgreSQL, MinIO
make start-processing   # Only Spark, Airflow

# Monitor
make logs              # All logs
make logs-kafka        # Kafka logs
make logs-spark        # Spark logs
make logs-airflow      # Airflow logs

# Management
make stop              # Stop all services
make restart           # Restart all
make status            # Service status
make health            # Health check

# Testing
make test-all          # Test all services
make test-kafka        # Test Kafka
make test-postgres     # Test PostgreSQL

# Utilities
make shell-kafka       # Kafka shell
make shell-postgres    # PostgreSQL shell
make shell-spark       # Spark shell
make backup-postgres   # Backup database
```

## Architecture at a Glance

```
┌──────────────┐
│ Data Sources │
│  (Datagen)   │
└──────┬───────┘
       │
       ▼
┌──────────────┐     ┌──────────────┐
│    Kafka     │────▶│    Spark     │
│ (3 brokers)  │     │  (Streaming) │
└──────────────┘     └──────┬───────┘
                            │
                            ▼
                    ┌──────────────┐
                    │   Iceberg    │
                    │ Bronze/Silver│
                    │    /Gold     │
                    └──────┬───────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
       ┌─────────────┐          ┌─────────────┐
       │ PostgreSQL  │          │  Superset   │
       │  (Serving)  │          │ (Analytics) │
       └─────────────┘          └─────────────┘
              │
              ▼
       ┌─────────────┐
       │   Airflow   │
       │(Orchestrate)│
       └─────────────┘
```

## Data Flow

1. **Datagen** generates synthetic banking transactions
2. **Kafka** receives and distributes events
3. **Spark Streaming** reads from Kafka, writes to Iceberg (Bronze)
4. **Airflow** orchestrates batch jobs:
   - Transform Bronze → Silver (cleaned data)
   - Aggregate Silver → Gold (business metrics)
   - Load Gold → PostgreSQL (serving layer)
5. **Superset** queries PostgreSQL for dashboards
6. **Grafana** monitors system health

## Sample Queries

### Spark SQL (Iceberg)

```python
# Connect via spark-shell
docker exec -it spark-master spark-shell

# Query Bronze layer
spark.sql("SELECT * FROM local.bronze.transactions LIMIT 5").show()

# Query Gold aggregations
spark.sql("""
    SELECT account_id, SUM(net_amount) as total
    FROM local.gold.daily_account_summary
    GROUP BY account_id
    ORDER BY total DESC
    LIMIT 10
""").show()
```

### PostgreSQL

```sql
-- Top accounts by transaction volume
SELECT account_id, SUM(total_transactions) as txn_count
FROM serving.account_summaries
GROUP BY account_id
ORDER BY txn_count DESC
LIMIT 10;

-- Daily transaction trends
SELECT transaction_date, COUNT(*), SUM(amount)
FROM serving.daily_transactions
WHERE transaction_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY transaction_date
ORDER BY transaction_date;

-- Merchant analytics
SELECT merchant, merchant_category,
       SUM(total_amount) as revenue,
       AVG(avg_amount) as avg_txn
FROM serving.merchant_analytics
GROUP BY merchant, merchant_category
ORDER BY revenue DESC
LIMIT 20;
```

## Troubleshooting

### Services won't start

```bash
# Check Docker resources
docker info

# Check logs
make logs

# Restart services
make restart
```

### Out of memory

```bash
# Use lightweight setup
docker-compose -f docker-compose.yml -f docker-compose.lightweight.yml up -d

# Or stop non-essential services
docker-compose stop superset datahub-*
```

### Kafka topics not created

```bash
# Manually create topics
make topics

# Verify
docker exec kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

### Datagen not producing data

```bash
# Reconfigure
make datagen

# Check connector status
curl http://localhost:8083/connectors/datagen-banking-transactions/status
```

### Airflow DAG not running

```bash
# Check scheduler
make logs-airflow

# Trigger manually
docker exec airflow-scheduler airflow dags trigger banking_data_pipeline
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

1. ✅ **Explore Kafka UI**: Monitor topics and messages
2. ✅ **Run Airflow DAG**: Execute the banking pipeline
3. ✅ **Query Data**: Use PostgreSQL or Spark SQL
4. ✅ **Create Dashboards**: Build visualizations in Superset
5. ✅ **Monitor System**: Check metrics in Grafana
6. ✅ **Customize Pipeline**: Modify Spark jobs and DAGs

## Getting Help

- Full documentation: [README.md](README.md)
- Architecture details: [ARCHITECTURE.md](ARCHITECTURE.md)
- Makefile commands: `make help`
- Logs: `make logs-[service-name]`

## Useful Resources

- Apache Kafka: https://kafka.apache.org/documentation/
- Apache Spark: https://spark.apache.org/docs/latest/
- Apache Iceberg: https://iceberg.apache.org/docs/latest/
- Apache Airflow: https://airflow.apache.org/docs/
- Apache Superset: https://superset.apache.org/docs/intro

---

**Ready to build real-time data pipelines? Start exploring!** 🚀
