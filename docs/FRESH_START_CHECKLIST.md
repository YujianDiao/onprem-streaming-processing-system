# Fresh Start Checklist

This document provides a complete checklist for starting the on-premise lakehouse system from scratch and verifying all components are working correctly.

## Prerequisites

### System Requirements

- [x] Docker 20.10+ installed
- [x] Docker Compose 2.0+ installed
- [x] Minimum 8 CPUs, 16GB RAM available
- [x] Recommended 16 CPUs, 32GB RAM

**Verification**:
```bash
docker --version
docker compose version
docker info | grep -E "CPUs|Total Memory"
```

### Disk Space

- [x] At least 20GB free disk space for Docker volumes
- [x] Additional space for data growth

**Verification**:
```bash
df -h
```

---

## Phase 1: Initial Setup

### 1.1 Clone and Navigate

```bash
cd onprem-streaming-processing-system
```

### 1.2 Verify Critical Files Exist

**Docker Configuration**:
- [x] `docker-compose.yml` exists
- [x] `superset/Dockerfile` exists
- [x] `hive-metastore/Dockerfile` exists
- [x] `hive-metastore/core-site.xml` exists (S3 credentials)
- [x] `data-generator/Dockerfile` exists

**Scripts**:
- [x] `submit-streaming-job.sh` exists and is executable
- [x] `submit-batch-job.sh` exists and is executable
- [x] `scripts/startup.sh` exists and is executable
- [x] `scripts/setup-kafka-topics.sh` exists and is executable

**Spark Jobs**:
- [x] `spark/jobs/kafka_to_iceberg_streaming.py` exists
- [x] `spark/jobs/batch_aggregations.py` exists

**Verification**:
```bash
ls -la submit-streaming-job.sh submit-batch-job.sh
ls -la scripts/*.sh
ls -la superset/Dockerfile hive-metastore/Dockerfile hive-metastore/core-site.xml
ls -la spark/jobs/*.py
```

### 1.3 Make Scripts Executable

```bash
chmod +x submit-streaming-job.sh submit-batch-job.sh scripts/*.sh
```

---

## Phase 2: Start Services

### 2.1 Start All Services

```bash
# Option 1: Using startup script (recommended for first time)
bash scripts/startup.sh

# Option 2: Direct docker compose
docker compose up -d
```

**Expected Duration**: 2-3 minutes for all services to initialize

### 2.2 Verify All Containers Are Running

```bash
docker compose ps
```

**Expected Services** (should show "Up" or "healthy"):
- [x] kafka-1, kafka-2, kafka-3
- [x] postgres
- [x] minio
- [x] hive-metastore
- [x] spark-master
- [x] spark-worker
- [x] trino
- [x] superset
- [x] schema-registry
- [x] kafka-ui
- [x] data-generator
- [x] prometheus
- [x] grafana

**Troubleshooting**:
```bash
# If any service is not running, check logs
docker logs <service-name>

# Common issues:
# - Out of memory: Reduce number of Spark workers
# - Port conflicts: Check if ports are already in use
```

### 2.3 Wait for Health Checks

Some services need time to initialize. Wait until health checks pass:

```bash
# Wait for Kafka
docker exec kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092

# Wait for PostgreSQL
docker exec postgres pg_isready -U hive

# Wait for Hive Metastore (may take 30-60 seconds)
docker logs hive-metastore | grep "Started HiveMetaStore"

# Wait for Trino
curl -f http://localhost:8080/v1/info

# Wait for Superset
curl -f http://localhost:8088/health
```

---

## Phase 3: Verify Infrastructure

### 3.1 Verify Kafka Cluster

```bash
# Check Kafka brokers
docker exec kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092

# List topics
docker exec kafka-1 kafka-topics.sh --list --bootstrap-server localhost:9092

# Expected: banking.transactions.raw (if data generator running)
```

**Web UI**: http://localhost:8080 (Kafka UI)

### 3.2 Verify MinIO Storage

```bash
# List buckets
docker exec minio mc ls local/

# Expected output should show 'lakehouse' bucket
```

**Web Console**: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin`

### 3.3 Verify Hive Metastore

```bash
# Check metastore logs
docker logs hive-metastore | tail -20

# Should see: "Started HiveMetaStore"

# Check PostgreSQL connection
docker exec postgres psql -U hive -d metastore -c "\dt"
```

### 3.4 Verify Trino Connection

```bash
# Test Trino CLI
docker exec trino trino --execute "SHOW CATALOGS;"

# Expected output: iceberg, system
```

**Web UI**: http://localhost:8080

### 3.5 Verify Superset

**Web UI**: http://localhost:8088
- Username: `admin`
- Password: `admin`

**Check Trino driver is installed**:
```bash
docker exec superset python -c "import trino; print('Trino driver OK')"
```

---

## Phase 4: Run End-to-End Data Pipeline

### 4.1 Generate Sample Data

```bash
# Generate 10,000 test transactions
docker exec kafka-broker-1 python /scripts/data_generator.py \
  --num-records 10000 \
  --topic banking.transactions.raw
```

**Expected Output**: "Sent 10000 records to topic banking.transactions.raw"

**Verification**:
```bash
# Check Kafka topic has messages
docker exec kafka-1 kafka-console-consumer.sh \
  --topic banking.transactions.raw \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

### 4.2 Start Streaming Pipeline

```bash
./submit-streaming-job.sh
```

**Expected Output**: "Job submitted in background"

**Wait 1-2 minutes for initial processing**

**Verification**:
```bash
# Check job is running
docker exec spark-master ps aux | grep kafka_to_iceberg

# View streaming logs
docker logs spark-master | tail -50

# Check Spark UI
open http://localhost:8081
```

### 4.3 Verify Bronze Layer Data

```bash
# Wait 2-3 minutes for micro-batches to process

# Check record count
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.bronze.transactions;"

# Expected: Should show ~10,000 records
```

**Full verification queries**:
```bash
docker exec -it trino trino --catalog iceberg --schema bronze
```

```sql
-- Count records
SELECT COUNT(*) FROM transactions;

-- Check data freshness
SELECT MAX(ingestion_timestamp) FROM transactions;

-- Verify transaction types
SELECT transaction_type, COUNT(*)
FROM transactions
GROUP BY transaction_type;

-- Exit with: quit
```

### 4.4 Run Batch Aggregations

```bash
./submit-batch-job.sh
```

**Expected Duration**: 1-2 minutes

**Verification**:
```bash
# Check silver layer
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.silver.transactions;"

# Check gold layers
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.gold.daily_account_summary;"

docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.gold.merchant_summary;"
```

### 4.5 Query Gold Layer

```bash
docker exec -it trino trino --catalog iceberg --schema gold
```

```sql
-- Top merchants by revenue
SELECT merchant, total_amount, total_transactions
FROM merchant_summary
ORDER BY total_amount DESC
LIMIT 10;

-- Daily account summary
SELECT account_id, transaction_date, net_change
FROM daily_account_summary
ORDER BY transaction_date DESC
LIMIT 20;
```

---

## Phase 5: Verify Superset Integration

### 5.1 Connect Superset to Trino

1. Open http://localhost:8088 (login: admin/admin)
2. Click **Settings** → **Database Connections** → **+ Database**
3. Select **Trino** from dropdown
4. Enter connection:
   - **Display Name**: `Iceberg Lakehouse`
   - **SQLAlchemy URI**: `trino://trino@trino:8080/iceberg`
5. Click **Test Connection** (should succeed)
6. Click **Connect**

**Verification**:
```bash
# Verify Trino driver is available
docker exec superset python -c "import trino; import sqlalchemy_trino; print('OK')"
```

### 5.2 Create a Dataset

1. Go to **Data** → **Datasets** → **+ Dataset**
2. Select:
   - **Database**: Iceberg Lakehouse
   - **Schema**: gold
   - **Table**: merchant_summary
3. Click **Create Dataset and Create Chart**

### 5.3 Test SQL Lab

1. Go to **SQL** → **SQL Lab**
2. Select database: **Iceberg Lakehouse**, schema: **gold**
3. Run test query:

```sql
SELECT merchant, total_transactions, total_amount
FROM merchant_summary
ORDER BY total_amount DESC
LIMIT 10;
```

**Expected**: Query should execute successfully and show results

---

## Phase 6: Monitoring and Health

### 6.1 Verify All Web UIs

- [x] Kafka UI: http://localhost:8080
- [x] Trino Web UI: http://localhost:8080
- [x] Spark Master UI: http://localhost:8081
- [x] Spark Worker UI: http://localhost:8082
- [x] Superset: http://localhost:8088
- [x] MinIO Console: http://localhost:9001
- [x] Grafana: http://localhost:3000
- [x] Prometheus: http://localhost:9090

**Quick access**:
```bash
make urls  # Show all URLs
```

### 6.2 Check Resource Usage

```bash
# Container resource usage
docker stats --no-stream

# Disk usage
docker system df
```

### 6.3 Verify Checkpoint Directory

```bash
# Check streaming checkpoint exists
docker exec minio mc ls local/lakehouse/checkpoints/kafka-streaming/

# Should show multiple committed batches
```

---

## Phase 7: Operational Verification

### 7.1 Test Continuous Streaming

```bash
# Generate more data
docker exec kafka-broker-1 python /scripts/data_generator.py \
  --num-records 5000 \
  --topic banking.transactions.raw

# Wait 1-2 minutes

# Verify new data in bronze
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.bronze.transactions;"

# Count should increase by ~5000
```

### 7.2 Test Iceberg Time Travel

```bash
docker exec trino trino --execute \
  "SELECT snapshot_id, committed_at
   FROM iceberg.bronze.transactions.snapshots
   ORDER BY committed_at DESC
   LIMIT 5;"
```

**Expected**: Should show multiple snapshots

### 7.3 Verify Hive Metastore Tables

```bash
docker exec postgres psql -U hive -d metastore -c \
  "SELECT TBL_NAME FROM TBLS WHERE TBL_NAME LIKE '%transactions%';"
```

**Expected**: Should show:
- bronze.transactions
- silver.transactions
- gold.daily_account_summary
- gold.merchant_summary

---

## Common Issues and Solutions

### Issue: Hive Metastore fails to start

**Symptoms**: `AccessDeniedException s3a://lakehouse`

**Solution**: Verify `hive-metastore/core-site.xml` exists and is copied in Dockerfile

```bash
# Check file exists
ls -la hive-metastore/core-site.xml

# Rebuild Hive Metastore
docker compose build --no-cache hive-metastore
docker compose up -d hive-metastore
```

### Issue: Streaming job terminates

**Symptoms**: Job exits after submission

**Solution**: Verify `submit-streaming-job.sh` uses detached execution (`docker exec -d`)

```bash
# Check script uses -d flag
grep "docker exec -d" submit-streaming-job.sh

# Resubmit job
./submit-streaming-job.sh
```

### Issue: Superset doesn't show Trino

**Symptoms**: Trino not in database dropdown

**Solution**: Rebuild Superset with Trino driver

```bash
# Verify Dockerfile exists
ls -la superset/Dockerfile

# Rebuild Superset
docker compose build --no-cache superset
docker compose up -d superset

# Wait 2 minutes, then check
docker exec superset python -c "import trino; print('OK')"
```

### Issue: Trino authentication error

**Symptoms**: `error 401: Basic authentication required`

**Solution**: Add username to connection URI

**Incorrect**: `trino://trino:8080/iceberg`
**Correct**: `trino://trino@trino:8080/iceberg`

### Issue: No Kafka consumer group visible

**This is EXPECTED behavior!**

Spark Structured Streaming uses **checkpoint-based offset management**, not Kafka consumer groups. Offsets are stored in MinIO at `s3a://lakehouse/checkpoints/kafka-streaming/`.

**Verification**:
```bash
docker exec minio mc ls local/lakehouse/checkpoints/kafka-streaming/offsets/
```

---

## Quick Commands Reference

### Service Management

```bash
# Start all services
docker compose up -d

# Stop all services (keep data)
docker compose stop

# Stop and remove containers (keep volumes)
docker compose down

# Complete cleanup (WARNING: deletes data)
docker compose down -v
```

### Data Pipeline

```bash
# Generate data
docker exec kafka-broker-1 python /scripts/data_generator.py --num-records 10000 --topic banking.transactions.raw

# Start streaming
./submit-streaming-job.sh

# Run batch aggregations
./submit-batch-job.sh

# Query data
docker exec -it trino trino --catalog iceberg --schema bronze
```

### Monitoring

```bash
# View logs
docker logs spark-master | tail -100
docker logs hive-metastore | tail -50
docker logs trino | tail -50

# Service status
docker compose ps

# Resource usage
docker stats --no-stream
```

### Troubleshooting

```bash
# Restart specific service
docker compose restart hive-metastore

# Rebuild custom images
docker compose build --no-cache hive-metastore superset data-generator

# View service health
docker compose ps | grep healthy
```

---

## Success Criteria

All checkboxes should be checked:

**Infrastructure**:
- [x] All containers running (docker compose ps shows "Up"/"healthy")
- [x] Kafka cluster accepting messages
- [x] MinIO storage accessible
- [x] Hive Metastore connected to PostgreSQL
- [x] Trino can query Iceberg catalog

**Data Pipeline**:
- [x] Data generator produces to Kafka topic
- [x] Streaming job processes Kafka → Bronze layer
- [x] Bronze table has data (SELECT COUNT(*) > 0)
- [x] Batch job creates Silver and Gold layers
- [x] Gold tables have aggregated data

**Analytics**:
- [x] Trino CLI can query all layers (bronze, silver, gold)
- [x] Superset connects to Trino successfully
- [x] SQL Lab executes queries successfully
- [x] Can create datasets and charts in Superset

**Monitoring**:
- [x] All web UIs accessible
- [x] Checkpoint directory exists in MinIO
- [x] Spark UI shows running applications
- [x] No error logs in critical services

---

## Makefile Quick Reference

```bash
make help              # Show all available commands
make setup             # Initial environment setup
make start             # Start all services
make status            # Show service status
make urls              # Show all service URLs
make logs              # View all logs
make test-all          # Test all services
make clean             # Stop and remove (with confirmation)
```

---

## Next Steps After Successful Validation

1. **Create Superset dashboards** for your use case
2. **Schedule batch jobs** using cron or workflow orchestration
3. **Add monitoring alerts** in Grafana
4. **Implement data quality checks**
5. **Scale Spark workers** as needed
6. **Customize data generator** for your domain
7. **Add more Gold layer aggregations**

---

## Documentation Reference

- **Full Architecture**: [ARCHITECTURE.md](../ARCHITECTURE.md)
- **Quick Start Guide**: [QUICK_START.md](../QUICK_START.md)
- **System Status**: [SYSTEM_STATUS.md](SYSTEM_STATUS.md)
- **Streaming Pipeline**: [STREAMING_PIPELINE_FIX.md](STREAMING_PIPELINE_FIX.md)
- **Batch Pipeline**: [BATCH_AGGREGATIONS_UPDATE.md](BATCH_AGGREGATIONS_UPDATE.md)
- **Superset Integration**: [SUPERSET_TRINO_INTEGRATION.md](SUPERSET_TRINO_INTEGRATION.md)

---

**Last Updated**: 2025-11-18
**Status**: ✅ All components validated and operational
