# On-Premise Data Streaming, Processing & Serving Platform

A comprehensive, containerized data platform designed for proof-of-concept deployments, featuring real-time streaming, batch processing, data lake storage, and business intelligence capabilities.

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

This platform provides a complete data engineering solution including:

- **Real-time Event Streaming**: Apache Kafka with Schema Registry
- **Stream & Batch Processing**: Apache Spark with Iceberg integration
- **Workflow Orchestration**: Apache Airflow
- **Data Lake Storage**: MinIO (S3-compatible) with Apache Iceberg
- **Serving Layer**: PostgreSQL with optimized tables
- **Data Catalog**: DataHub (optional)
- **Monitoring**: Prometheus & Grafana
- **Analytics**: Apache Superset

### Key Features

✅ Fully containerized with Docker Compose
✅ Medallion architecture (Bronze/Silver/Gold layers)
✅ ACID transactions and time travel with Iceberg
✅ Schema evolution and validation
✅ Automated data quality checks
✅ Comprehensive monitoring and alerting
✅ Sample banking data pipeline included

## Architecture

The platform implements a modern data lakehouse architecture with the following layers:

```
Data Sources → Kafka → Spark Streaming → Iceberg (Bronze/Silver/Gold) → PostgreSQL → Analytics
                ↓                                    ↓
           Schema Registry                     Airflow (Orchestration)
                                                     ↓
                                         Superset/Grafana (Visualization)
```

For detailed architecture documentation, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Event Streaming | Apache Kafka | 7.5.0 | Message broker and event backbone |
| Coordination | Zookeeper | 7.5.0 | Kafka cluster coordination |
| Schema Management | Confluent Schema Registry | 7.5.0 | Avro/JSON schema validation |
| Data Integration | Kafka Connect | 7.5.0 | Source/sink connectors |
| Stream Processing | Apache Spark | 3.5.0 | Unified batch and streaming |
| Orchestration | Apache Airflow | 2.7.3 | Workflow management |
| Object Storage | MinIO | Latest | S3-compatible data lake |
| Table Format | Apache Iceberg | 1.4.2 | ACID transactions, time travel |
| Relational DB | PostgreSQL | 15 | Serving layer |
| Data Catalog | DataHub | Latest | Metadata management (optional) |
| Monitoring | Prometheus | Latest | Metrics collection |
| Dashboards | Grafana | Latest | System monitoring |
| BI & Analytics | Apache Superset | 3.0.0 | Data visualization |

## Prerequisites

### Hardware Requirements

**Minimum (for testing):**
- CPU: 8 cores
- RAM: 16 GB
- Storage: 100 GB SSD

**Recommended (for PoC):**
- CPU: 16+ cores
- RAM: 32+ GB
- Storage: 500 GB SSD
- Network: 1 Gbps

### Software Requirements

- Docker Engine 20.10+
- Docker Compose 2.0+
- Git
- Bash shell
- curl
- jq (optional, for JSON processing)

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

# 2. Copy environment file
cp .env.example .env

# 3. Make scripts executable
chmod +x scripts/*.sh

# 4. Start all services
bash scripts/startup.sh
```

The startup script will:
1. Start infrastructure services (Kafka, PostgreSQL, MinIO)
2. Initialize storage and topics
3. Configure data generation
4. Start processing and orchestration layers
5. Launch monitoring and analytics tools

**Access the platform:**
- Kafka UI: http://localhost:8080
- Airflow: http://localhost:8085 (admin/admin)
- Spark Master: http://localhost:8888
- Superset: http://localhost:8088 (admin/admin)
- Grafana: http://localhost:3000 (admin/admin)
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

## Detailed Setup

### Step 1: Infrastructure Services

Start the core infrastructure:

```bash
docker-compose up -d zookeeper kafka-1 kafka-2 kafka-3 postgres minio
```

Verify services are healthy:

```bash
docker-compose ps
```

### Step 2: Initialize Storage

Create MinIO buckets:

```bash
docker-compose up minio-init
```

Initialize PostgreSQL schemas:

```bash
docker exec -it postgres psql -U admin -d metastore -c "\dn"
```

### Step 3: Kafka Ecosystem

Start Kafka components:

```bash
docker-compose up -d schema-registry kafka-connect kafka-ui
```

Create topics:

```bash
bash scripts/setup-kafka-topics.sh
```

Configure data generation:

```bash
bash scripts/configure-datagen.sh
```

### Step 4: Processing Layer

Start Spark cluster:

```bash
docker-compose up -d spark-master spark-worker-1 spark-worker-2
```

Check Spark UI: http://localhost:8888

### Step 5: Orchestration

Start Airflow:

```bash
docker-compose up -d airflow-webserver airflow-scheduler
```

Access Airflow UI: http://localhost:8085

### Step 6: Monitoring & Analytics

Start monitoring stack:

```bash
docker-compose up -d prometheus grafana superset
```

### Step 7: Data Catalog (Optional)

Start DataHub:

```bash
docker-compose --profile datahub up -d
```

## Usage Guide

### Running the Banking Data Pipeline

The platform includes a complete banking data pipeline that demonstrates:
- Real-time transaction processing
- Data quality validation
- Medallion architecture (Bronze → Silver → Gold)
- Serving layer population

#### Trigger the Pipeline

1. **Access Airflow**: http://localhost:8085
2. **Enable DAG**: Toggle `banking_data_pipeline` to ON
3. **Trigger**: Click play button to run manually
4. **Monitor**: Watch task execution in Graph view

#### Pipeline Stages

```
1. Check Kafka Topics → Validate data source
2. Validate MinIO → Ensure storage is ready
3. Spark Streaming → Read from Kafka, write to Bronze layer
4. Batch Aggregation → Transform Bronze → Silver → Gold
5. Load to PostgreSQL → Populate serving tables
6. Data Quality Checks → Validate results
```

### Querying Data

#### Query Iceberg Tables with Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .getOrCreate()

# Query Bronze layer
spark.sql("SELECT * FROM local.bronze.transactions LIMIT 10").show()

# Query Gold layer aggregations
spark.sql("""
    SELECT account_id, SUM(total_transactions) as txn_count
    FROM local.gold.daily_account_summary
    GROUP BY account_id
    ORDER BY txn_count DESC
    LIMIT 10
""").show()
```

#### Query PostgreSQL Serving Layer

```bash
docker exec -it postgres psql -U admin -d metastore

-- Query daily transactions
SELECT transaction_date, COUNT(*), SUM(amount)
FROM serving.daily_transactions
WHERE transaction_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY transaction_date
ORDER BY transaction_date DESC;

-- Query account summaries
SELECT * FROM serving.account_summaries
WHERE total_transactions > 100
ORDER BY net_amount DESC
LIMIT 10;
```

### Creating Superset Dashboards

1. **Login**: http://localhost:8088 (admin/admin)
2. **Add Database Connection**:
   - Database: `postgresql://admin:admin123@postgres:5432/metastore`
   - Test connection
3. **Create Dataset**: Select `serving` schema tables
4. **Build Charts**: Use the chart builder
5. **Create Dashboard**: Combine multiple charts

### Monitoring with Grafana

1. **Access**: http://localhost:3000 (admin/admin)
2. **Add Prometheus Data Source**: http://prometheus:9090
3. **Import Dashboards**:
   - Kafka metrics
   - Spark metrics
   - System metrics

## Monitoring & Operations

### Health Checks

Check all services:

```bash
docker-compose ps
```

Check specific service logs:

```bash
docker-compose logs -f kafka-1
docker-compose logs -f spark-master
docker-compose logs -f airflow-scheduler
```

### Kafka Operations

List topics:

```bash
docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092
```

Describe topic:

```bash
docker exec kafka-1 kafka-topics --describe \
    --topic banking.transactions.raw \
    --bootstrap-server kafka-1:29092
```

Consume messages:

```bash
docker exec kafka-1 kafka-console-consumer \
    --topic banking.transactions.raw \
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
    /opt/bitnami/spark/jobs/your_job.py
```

Monitor Spark applications:

```bash
docker exec spark-master spark-submit --status <submission-id>
```

### Airflow Operations

List DAGs:

```bash
docker exec airflow-scheduler airflow dags list
```

Trigger DAG manually:

```bash
docker exec airflow-scheduler airflow dags trigger banking_data_pipeline
```

View task logs:

```bash
docker exec airflow-scheduler airflow tasks logs banking_data_pipeline check_kafka_topics 2024-01-01
```

### Backup & Recovery

Backup PostgreSQL:

```bash
docker exec postgres pg_dump -U admin metastore > backup_$(date +%Y%m%d).sql
```

Backup MinIO data:

```bash
docker exec minio-client mc mirror myminio/lakehouse ./backup/lakehouse
```

## Troubleshooting

### Common Issues

#### Kafka broker not connecting

```bash
# Check Zookeeper
docker exec zookeeper zkServer.sh status

# Check Kafka logs
docker-compose logs kafka-1 | grep ERROR

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

#### Airflow DAG not running

```bash
# Check scheduler logs
docker-compose logs airflow-scheduler

# Verify database connection
docker exec airflow-scheduler airflow db check

# Reset DAG
docker exec airflow-scheduler airflow dags unpause banking_data_pipeline
```

#### Out of memory errors

```bash
# Increase Docker memory allocation
# Edit .env and increase memory limits

# Stop non-essential services
docker-compose stop datahub-gms datahub-frontend superset

# Restart with more resources
docker-compose up -d
```

### Logs Location

- Airflow logs: `./airflow/logs/`
- Spark logs: Check Spark UI at http://localhost:8888
- Kafka logs: `docker-compose logs kafka-1`
- System logs: `docker-compose logs`

## Performance Tuning

### Kafka Optimization

```yaml
# In docker-compose.yml, increase Kafka memory:
environment:
  KAFKA_HEAP_OPTS: "-Xmx2G -Xms2G"
```

### Spark Optimization

```python
# Increase executor memory in Airflow DAG:
spark_streaming_job = SparkSubmitOperator(
    executor_memory='4g',
    driver_memory='2g',
    total_executor_cores=4
)
```

### PostgreSQL Tuning

```sql
-- Connect to PostgreSQL
docker exec -it postgres psql -U admin -d metastore

-- Increase shared buffers
ALTER SYSTEM SET shared_buffers = '2GB';
ALTER SYSTEM SET effective_cache_size = '8GB';
ALTER SYSTEM SET work_mem = '64MB';

-- Restart PostgreSQL
docker-compose restart postgres
```

### MinIO Performance

```bash
# Use distributed mode for production
# Add more drives to docker-compose.yml:
volumes:
  - minio-data-1:/data1
  - minio-data-2:/data2
  - minio-data-3:/data3
  - minio-data-4:/data4
```

## Scaling Considerations

### Horizontal Scaling

- **Kafka**: Add more brokers, increase partitions
- **Spark**: Add more worker nodes
- **MinIO**: Add nodes to distributed cluster

### Vertical Scaling

- Increase container resource limits
- Optimize JVM heap sizes
- Tune database parameters

## Security Best Practices

1. **Change default passwords** in `.env` file
2. **Enable SSL/TLS** for production
3. **Implement authentication** (SASL for Kafka, LDAP for Airflow)
4. **Use secrets management** (Docker secrets, HashiCorp Vault)
5. **Network isolation** with Docker networks
6. **Regular backups** of data and metadata

## Cleanup

Stop all services:

```bash
docker-compose down
```

Remove volumes (WARNING: deletes all data):

```bash
docker-compose down -v
```

Remove images:

```bash
docker-compose down --rmi all
```

Complete cleanup:

```bash
docker-compose down -v --rmi all
docker system prune -a
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
- Email: [your-email]

## Acknowledgments

Built with open-source technologies:
- Apache Software Foundation
- Confluent Platform
- MinIO
- Grafana Labs
- And many more amazing projects!

---

**Note**: This is a proof-of-concept platform. For production deployments, additional security hardening, high availability configurations, and performance tuning are required.
