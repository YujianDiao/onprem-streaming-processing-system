# On-Premise Data Lakehouse Platform

A modern, containerized data lakehouse platform designed for proof-of-concept deployments, featuring real-time streaming ingestion, Apache Iceberg storage, and distributed SQL analytics.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup](#detailed-setup)
- [Usage Guide](#usage-guide)
- [Monitoring & Operations](#monitoring--operations)
- [Troubleshooting](#troubleshooting)
- [Performance Tuning](#performance-tuning)
- [Contributing](#contributing)

## Overview

This platform provides a complete data lakehouse solution including:

- **Real-time Event Streaming**: Apache Kafka in KRaft mode (no Zookeeper)
- **Stream Processing**: Apache Spark with Iceberg integration
- **Data Lake Storage**: MinIO (S3-compatible) with Apache Iceberg
- **SQL Analytics**: Trino for distributed queries on the lakehouse
- **Metadata Management**: Hive Metastore for Iceberg catalog
- **Streaming Data Generator**: Python-based fake data generation
- **Data Catalog**: DataHub (optional)
- **Monitoring**: Prometheus & Grafana
- **Management UI**: Kafka UI for cluster monitoring

### Key Features

✅ Fully containerized with Docker Compose
✅ Modern data lakehouse architecture
✅ Kafka in KRaft mode (no Zookeeper dependency)
✅ ACID transactions and time travel with Iceberg
✅ Distributed SQL queries with Trino
✅ Schema evolution and validation
✅ Real-time streaming data generation
✅ Comprehensive monitoring and alerting
✅ Sample banking data pipeline included

## Architecture

The platform implements a modern data lakehouse architecture optimized for analytics:

```
Data Generator → Kafka → Spark Streaming → Iceberg (MinIO) ← Trino (SQL Analytics)
                  ↓                             ↑
            Schema Registry              Hive Metastore
                                              ↓
                                    Prometheus/Grafana (Monitoring)
```

**Key Architectural Decisions:**

- **No Orchestration Layer**: Spark jobs run continuously for streaming, no Airflow needed
- **No Serving Layer**: Trino queries Iceberg tables directly, no PostgreSQL copy
- **Simplified Stack**: Removed Zookeeper, Kafka Connect, Superset for reduced complexity
- **Lakehouse-First**: All data stored in open format (Iceberg/Parquet) on object storage

For detailed architecture documentation, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Event Streaming | Apache Kafka (KRaft) | 7.5.0 | Message broker in consensus mode |
| Schema Management | Confluent Schema Registry | 7.5.0 | Avro/JSON schema validation |
| Stream Processing | Apache Spark | 3.5.0 | Real-time data processing (apache/spark) |
| SQL Analytics | Trino | Latest | Distributed SQL query engine |
| Object Storage | MinIO | Latest | S3-compatible data lake |
| Table Format | Apache Iceberg | 1.4.2 | ACID transactions, time travel |
| Metadata Store | Hive Metastore | 4.0.0 | Iceberg catalog management |
| Metastore DB | PostgreSQL | 15 | Hive Metastore backend |
| Data Generator | Python/Faker | Custom | Streaming data generation |
| Data Catalog | DataHub | Latest | Metadata management (optional) |
| Monitoring | Prometheus | Latest | Metrics collection |
| Dashboards | Grafana | Latest | System monitoring |
| Kafka UI | Provectus Kafka UI | Latest | Kafka cluster management |

## Prerequisites

### Hardware Requirements

**Minimum (for testing):**
- CPU: 8 cores
- RAM: 12 GB
- Storage: 50 GB SSD

**Recommended (for PoC):**
- CPU: 16+ cores
- RAM: 24+ GB
- Storage: 200 GB SSD
- Network: 1 Gbps

### Software Requirements

- Docker Engine 20.10+
- Docker Compose 2.0+
- Git
- Bash shell
- curl

### Operating System

- Linux (Ubuntu 20.04+, CentOS 8+)
- macOS 11+ (with Docker Desktop)
- Windows 10/11 (with WSL2 and Docker Desktop)

## Quick Start

Get the platform up and running in 5 minutes:

```bash
# 1. Clone the repository
git clone <repository-url>
cd onprem-streaming-processing-system

# 2. Initial setup
make setup

# 3. Start all services
make start
```

The startup script will:
1. Start Kafka cluster (KRaft mode - no Zookeeper)
2. Initialize MinIO buckets for Iceberg storage
3. Start Hive Metastore for catalog management
4. Launch Spark cluster for streaming processing
5. Start Trino for SQL analytics
6. Deploy monitoring stack (Prometheus/Grafana)
7. Start data generator for streaming data

**Access the platform:**
- Kafka UI: http://localhost:8080
- Schema Registry: http://localhost:8081
- Trino: http://localhost:8086
- Spark Master: http://localhost:8888
- Spark Worker 1: http://localhost:8091
- Spark Worker 2: http://localhost:8092
- Grafana: http://localhost:3000 (admin/admin)
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
- Prometheus: http://localhost:9090

## Detailed Setup

### Step 1: Infrastructure Services

Start the core infrastructure:

```bash
make start-core
# Or manually:
# docker-compose up -d kafka-1 kafka-2 kafka-3 postgres minio hive-metastore
```

Verify services are healthy:

```bash
make status
# Or: docker-compose ps
```

### Step 2: Initialize Storage

Create MinIO buckets:

```bash
docker-compose up minio-init
```

Verify Hive Metastore:

```bash
docker exec -it postgres psql -U hive -d metastore -c "\l"
```

### Step 3: Kafka Ecosystem

Start Kafka components:

```bash
docker-compose up -d schema-registry kafka-ui
```

Create topics:

```bash
bash scripts/setup-kafka-topics.sh
```

### Step 4: Processing Layer

Start Spark cluster:

```bash
make start-processing
# Or: docker-compose up -d spark-master spark-worker-1 spark-worker-2
```

Check Spark UI: http://localhost:8888

### Step 5: Analytics Layer

Start Trino:

```bash
make start-analytics
# Or: docker-compose up -d trino
```

Access Trino UI: http://localhost:8086

### Step 6: Monitoring Stack

Start monitoring:

```bash
make start-monitoring
# Or: docker-compose up -d prometheus grafana
```

### Step 7: Data Generator

Start the streaming data generator:

```bash
make datagen-start
# Or: docker-compose up -d data-generator
```

Monitor data generation:

```bash
make logs-datagen
```

### Step 8: Data Catalog (Optional)

Start DataHub for data governance:

```bash
make start-datahub
# Or: docker-compose --profile datahub up -d
```

## Usage Guide

### Querying the Data Lakehouse with Trino

#### Connect to Trino CLI

```bash
make shell-trino
# Or: docker exec -it trino trino
```

#### Basic Queries

```sql
-- Show available catalogs
SHOW CATALOGS;

-- Show schemas in Iceberg catalog
SHOW SCHEMAS IN iceberg;

-- List tables
SHOW TABLES IN iceberg.default;

-- Query raw transaction data
SELECT * FROM iceberg.default.transactions LIMIT 10;

-- Aggregate queries
SELECT
    transaction_type,
    COUNT(*) as count,
    SUM(amount) as total_amount
FROM iceberg.default.transactions
WHERE transaction_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY transaction_type
ORDER BY total_amount DESC;

-- Fraud detection query
SELECT
    customer_id,
    COUNT(*) as suspicious_txn_count,
    SUM(amount) as total_amount
FROM iceberg.default.transactions
WHERE is_fraud = true
  AND transaction_date >= CURRENT_DATE - INTERVAL '1' DAY
GROUP BY customer_id
ORDER BY total_amount DESC;
```

#### Time Travel Queries with Iceberg

```sql
-- Query historical snapshot
SELECT * FROM iceberg.default.transactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00'
LIMIT 10;

-- Show table history
SELECT * FROM iceberg.default.transactions$snapshots;

-- Query by snapshot ID
SELECT * FROM iceberg.default.transactions
FOR SYSTEM_VERSION AS OF 1234567890;
```

### Monitoring Data Flow

#### Check Kafka Topics

Access Kafka UI at http://localhost:8080 to:
- View topic list and message counts
- Browse messages in real-time
- Monitor consumer lag
- View cluster health

#### Monitor Spark Jobs

Access Spark UI at http://localhost:8888 to:
- View running streaming jobs
- Check executor status
- Monitor job progress
- View logs and metrics

#### Query Iceberg Metadata

```sql
-- In Trino CLI
-- Show table files
SELECT * FROM iceberg.default.transactions$files;

-- Show table partitions
SELECT * FROM iceberg.default.transactions$partitions;

-- Table statistics
SELECT * FROM iceberg.default.transactions$manifests;
```

### Monitoring with Grafana

1. **Access**: http://localhost:3000 (admin/admin)
2. **Add Prometheus Data Source**:
   - URL: http://prometheus:9090
   - Click "Save & Test"
3. **Import Dashboards**:
   - Kafka JMX metrics
   - Spark metrics
   - System metrics
   - MinIO metrics

### Working with the Data Generator

Control data generation:

```bash
# Start generating data
make datagen-start

# View generator logs
make logs-datagen

# Stop data generation
make datagen-stop
```

Customize generation rate in `.env`:

```bash
DATA_GENERATOR_TOPIC=banking-transactions
DATA_GENERATOR_INTERVAL_MS=1000  # Generate every 1 second
```

## Monitoring & Operations

### Health Checks

Check all services:

```bash
make status
# Or: docker-compose ps
```

Test specific services:

```bash
make test-kafka
make test-postgres
make test-minio
make test-trino
make test-all  # Test everything
```

View logs:

```bash
make logs-kafka
make logs-spark
make logs-trino
make logs-datagen
# Or: make logs (all services)
```

### Kafka Operations

List topics:

```bash
docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092
```

Describe topic:

```bash
docker exec kafka-1 kafka-topics --describe \
    --topic banking-transactions \
    --bootstrap-server kafka-1:29092
```

Consume messages:

```bash
docker exec kafka-1 kafka-console-consumer \
    --topic banking-transactions \
    --bootstrap-server kafka-1:29092 \
    --from-beginning \
    --max-messages 10
```

### Spark Operations

Submit custom Spark job:

```bash
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 \
    /opt/bitnami/spark/jobs/your_job.py
```

Monitor running jobs:

```bash
# Check Spark UI
open http://localhost:8888

# View worker logs
docker-compose logs -f spark-worker-1
```

### Trino Operations

Execute SQL from command line:

```bash
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.default.transactions"
```

Check Trino cluster status:

```bash
curl http://localhost:8086/v1/info | jq
```

### Backup & Recovery

Backup PostgreSQL (Hive Metastore):

```bash
make backup-postgres
# Or: docker exec postgres pg_dump -U hive metastore > backup_$(date +%Y%m%d).sql
```

Backup MinIO data:

```bash
make backup-minio
# Or: docker exec minio-client mc mirror myminio/lakehouse ./backup/lakehouse
```

## Troubleshooting

### Common Issues

#### Kafka broker not connecting

```bash
# Check Kafka logs (KRaft mode - no Zookeeper)
docker-compose logs kafka-1 | grep ERROR

# Verify Kafka cluster
make test-kafka

# Restart Kafka cluster
docker-compose restart kafka-1 kafka-2 kafka-3
```

#### Spark job failing

```bash
# Check worker logs
docker-compose logs spark-worker-1

# Verify MinIO connectivity
docker exec spark-master curl http://minio:9000/minio/health/live

# Check resource allocation
docker stats
```

#### Trino query failing

```bash
# Check Trino logs
docker-compose logs trino

# Verify Hive Metastore connection
docker-compose logs hive-metastore

# Test Trino connectivity
make test-trino
```

#### Out of memory errors

```bash
# Check resource usage
docker stats

# Stop non-essential services
docker-compose stop datahub-mysql datahub-elasticsearch datahub-gms datahub-frontend

# Use lightweight configuration
docker-compose -f docker-compose.yml -f docker-compose.lightweight.yml up -d
```

#### Data not appearing in Trino

```bash
# Check if data generator is running
docker-compose ps data-generator

# Verify Kafka messages
docker exec kafka-1 kafka-console-consumer \
    --topic banking-transactions \
    --bootstrap-server kafka-1:29092 \
    --max-messages 5

# Check Spark streaming job logs
docker-compose logs spark-master | grep streaming

# Verify Hive Metastore has tables
docker exec trino trino --execute "SHOW TABLES IN iceberg.default"
```

### Logs Location

- Spark logs: Check Spark UI at http://localhost:8888
- Kafka logs: `docker-compose logs kafka-1`
- Trino logs: `docker-compose logs trino`
- Data Generator logs: `docker-compose logs data-generator`
- System logs: `docker-compose logs`

## Performance Tuning

### Kafka Optimization

```yaml
# In docker-compose.yml, increase Kafka memory:
environment:
  KAFKA_HEAP_OPTS: "-Xmx2G -Xms2G"
  # Increase partitions for better parallelism
  KAFKA_NUM_PARTITIONS: 6
```

### Spark Optimization

Edit Spark configuration in `spark/conf/spark-defaults.conf`:

```properties
spark.executor.memory=4g
spark.driver.memory=2g
spark.executor.cores=2
spark.sql.shuffle.partitions=200
spark.streaming.kafka.maxRatePerPartition=1000
```

### Trino Tuning

Increase Trino memory in `trino/etc/config.properties`:

```properties
query.max-memory=4GB
query.max-memory-per-node=2GB
query.max-total-memory-per-node=3GB
```

### MinIO Performance

```bash
# Use distributed mode for production
# Configure in docker-compose.yml:
command: server /data{1...4} --console-address ":9001"
volumes:
  - minio-data-1:/data1
  - minio-data-2:/data2
  - minio-data-3:/data3
  - minio-data-4:/data4
```

### Lightweight Mode for Limited Resources

Use the lightweight compose file:

```bash
docker-compose -f docker-compose.yml -f docker-compose.lightweight.yml up -d
```

This configuration:
- Runs single Kafka broker instead of 3
- Reduces Spark worker memory to 2G
- Reduces Trino memory allocation
- Reduces PostgreSQL connection pool

## Scaling Considerations

### Horizontal Scaling

- **Kafka**: Add more brokers by creating additional kafka-N services
- **Spark**: Add more worker nodes to the cluster
- **Trino**: Add worker nodes (configure coordinator vs worker mode)
- **MinIO**: Deploy in distributed mode with multiple nodes

### Vertical Scaling

- Increase container resource limits in docker-compose.yml
- Optimize JVM heap sizes for Kafka, Spark, Trino
- Tune PostgreSQL parameters for Hive Metastore

## Security Best Practices

1. **Change default passwords** in `.env` file
2. **Enable SSL/TLS** for Kafka, Trino, MinIO in production
3. **Implement authentication**:
   - SASL for Kafka
   - LDAP/OAuth for Trino
   - IAM policies for MinIO
4. **Use secrets management** (Docker secrets, HashiCorp Vault)
5. **Network isolation** with Docker networks
6. **Regular backups** of Hive Metastore and Iceberg metadata
7. **Encrypt data at rest** in MinIO
8. **Enable audit logging** in Trino

## Makefile Commands

The project includes a comprehensive Makefile for common operations:

```bash
make help              # Show all available commands
make setup             # Initial setup
make start             # Start all services
make stop              # Stop all services
make restart           # Restart all services
make status            # Show service status
make logs              # View all logs

# Service-specific
make start-core        # Start core services
make start-processing  # Start Spark
make start-analytics   # Start Trino
make start-monitoring  # Start Prometheus/Grafana

# UI access
make kafka-ui          # Open Kafka UI
make trino-ui          # Open Trino UI
make spark-ui          # Open Spark UI
make grafana-ui        # Open Grafana
make minio-ui          # Open MinIO Console
make all-ui            # Open all UIs

# Operations
make topics            # Create Kafka topics
make datagen-start     # Start data generator
make datagen-stop      # Stop data generator
make test-all          # Test all services
make backup-postgres   # Backup Hive Metastore
make backup-minio      # Backup Iceberg data

# Shells
make shell-kafka       # Kafka shell
make shell-spark       # Spark shell
make shell-trino       # Trino CLI
make shell-postgres    # PostgreSQL shell

# Cleanup
make clean             # Stop and remove volumes
make clean-all         # Complete cleanup
```

## Service URLs Reference

| Service | URL | Default Credentials |
|---------|-----|-------------------|
| Kafka UI | http://localhost:8080 | - |
| Schema Registry | http://localhost:8081 | - |
| Trino | http://localhost:8086 | - |
| Spark Master | http://localhost:8888 | - |
| Spark Worker 1 | http://localhost:8091 | - |
| Spark Worker 2 | http://localhost:8092 | - |
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin |
| Grafana | http://localhost:3000 | admin/admin |
| Prometheus | http://localhost:9090 | - |
| PostgreSQL | localhost:5432 | hive/hive123 |
| Hive Metastore | thrift://localhost:9083 | - |
| DataHub (optional) | http://localhost:9002 | - |

## Cleanup

Stop all services:

```bash
make stop
# Or: docker-compose down
```

Remove volumes (WARNING: deletes all data):

```bash
make clean
# Or: docker-compose down -v
```

Complete cleanup with confirmation:

```bash
make clean-all
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Specify your license here]

## Support

For issues and questions:
- GitHub Issues: [repository-url]/issues
- Documentation: [ARCHITECTURE.md](ARCHITECTURE.md)

## Acknowledgments

Built with open-source technologies:
- Apache Software Foundation (Kafka, Spark, Iceberg, Hive)
- Trino Foundation
- Confluent Platform
- MinIO
- Grafana Labs
- And many more amazing projects!

---

**Note**: This is a proof-of-concept platform demonstrating modern data lakehouse architecture. For production deployments, additional security hardening, high availability configurations, and performance tuning are required.

## What's Different from Traditional Data Lakes?

This lakehouse platform provides:

✅ **Direct SQL queries** on data lake (no ETL to warehouse needed)
✅ **ACID transactions** with Iceberg (reliable updates/deletes)
✅ **Time travel** and data versioning out of the box
✅ **Unified batch and streaming** processing
✅ **Schema evolution** without downtime
✅ **Open data formats** (no vendor lock-in)
✅ **Simplified architecture** (fewer moving parts)

This eliminates the need for separate data warehouse, serving layers, and complex orchestration!
