# On-Premise Data Lakehouse Platform - Architecture Design

## Executive Summary

This document outlines a **modern data lakehouse platform** designed for on-premise, single-server deployments. The system leverages cutting-edge open-source technologies to enable direct SQL analytics on a data lake with ACID guarantees, eliminating the need for separate data warehouses and complex orchestration layers.

**Key Innovation**: Query your data lake directly with SQL while maintaining ACID transactions, time travel, and schema evolution—all running on a single server.

## System Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     DATA GENERATION & INGESTION LAYER                     │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌──────────────────┐      ┌──────────────────┐      ┌────────────────┐ │
│  │ Data Generator   │─────▶│  Apache Kafka    │◀────▶│ Schema Registry│ │
│  │   (Python)       │      │  (3 Brokers)     │      │  (Avro/JSON)   │ │
│  │   Faker Library  │      │  KRaft Mode      │      └────────────────┘ │
│  └──────────────────┘      └──────────────────┘                          │
│                                                                            │
│  • No Zookeeper (Kafka in KRaft consensus mode)                          │
│  • Realistic banking transaction data                                     │
│  • Schema validation at ingestion                                         │
└──────────────────────────────────────────────────────────────────────────┘
                                   │
                                   │ Stream Events
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                          PROCESSING LAYER                                 │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                    Apache Spark Cluster                             │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐            │ │
│  │  │ Spark Master │  │ Spark Worker │  │ Spark Worker │            │ │
│  │  │ (Container)  │  │      1       │  │      2       │            │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘            │ │
│  │                                                                     │ │
│  │  • Continuous Spark Streaming (no batch orchestration)            │ │
│  │  • Kafka → Iceberg writes with ACID                               │ │
│  │  • Automatic checkpointing and recovery                           │ │
│  │  • All workers run on same server (Docker networking)             │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
│  • No Airflow needed (continuous streaming, not batch)                   │
│  • Simplified operations and monitoring                                   │
└──────────────────────────────────────────────────────────────────────────┘
                                   │
                                   │ Writes to Lakehouse
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     LAKEHOUSE STORAGE LAYER                               │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                   MinIO (S3-Compatible Storage)                    │  │
│  │                                                                     │  │
│  │  ┌─────────────────────────────────────────────────────────────┐ │  │
│  │  │         Apache Iceberg Tables (ACID Data Lake)              │ │  │
│  │  │                                                               │ │  │
│  │  │  • Transactional writes (ACID guarantees)                   │ │  │
│  │  │  • Time travel (query historical snapshots)                 │ │  │
│  │  │  • Schema evolution (add/modify columns)                    │ │  │
│  │  │  • Parquet data files + JSON metadata                       │ │  │
│  │  │  • Partition evolution without rewrite                      │ │  │
│  │  └─────────────────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │              Hive Metastore + PostgreSQL                          │  │
│  │                                                                     │  │
│  │  • Table schemas and partitions                                   │  │
│  │  • Iceberg snapshot metadata                                      │  │
│  │  • Location pointers to MinIO                                     │  │
│  │  • Lightweight metadata only (no data)                            │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│  • No separate serving layer (query lakehouse directly)                  │
│  • Single source of truth                                                 │
└──────────────────────────────────────────────────────────────────────────┘
                                   │
                                   │ Query via Trino
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                      SQL ANALYTICS LAYER                                  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                        Trino (SQL Engine)                           │ │
│  │                                                                      │ │
│  │  • Distributed SQL queries on Iceberg tables                       │ │
│  │  • Standard SQL interface (ANSI SQL)                               │ │
│  │  • Time travel queries (FOR SYSTEM_TIME AS OF)                     │ │
│  │  • Metadata exploration (snapshots, files, partitions)             │ │
│  │  • Sub-second queries on billions of rows                          │ │
│  │  • No ETL needed from data lake                                    │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
│  • Replaces traditional BI tools and serving layers                      │
│  • Direct access to lakehouse                                             │
└──────────────────────────────────────────────────────────────────────────┘
                                   │
                                   │ Monitoring
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                  MONITORING & MANAGEMENT LAYER                            │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐       │
│  │    Kafka UI      │  │   Prometheus     │  │     Grafana      │       │
│  │  (Kafka Monitor) │  │  (Metrics Store) │  │   (Dashboards)   │       │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘       │
│                                                                            │
│  ┌──────────────────┐                                                     │
│  │    DataHub       │  (Optional - Data Governance)                      │
│  │  (Data Catalog)  │                                                     │
│  └──────────────────┘                                                     │
│                                                                            │
│  • Real-time metrics collection                                           │
│  • System health dashboards                                               │
│  • Data lineage tracking (optional)                                       │
└──────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Patterns

### 1. Real-Time Ingestion Flow

```
Data Generator → Kafka Topic → Spark Streaming → Iceberg Table → Trino Query
     (1s)          (ms)           (2-3s)            (append)       (sub-second)

1. Generator creates synthetic banking transactions
2. Kafka receives and distributes across partitions
3. Spark Streaming reads micro-batches
4. Iceberg writes with ACID guarantees
5. Trino queries latest data instantly
```

### 2. Query Flow (Direct Lakehouse Access)

```
User SQL Query → Trino → Hive Metastore → Iceberg Metadata → MinIO (Parquet)
                           (schema)         (file locations)    (data files)

1. User submits SQL via Trino CLI/UI
2. Trino checks Hive Metastore for table schema
3. Iceberg provides metadata and file locations
4. Trino reads Parquet files directly from MinIO
5. Results returned to user

**No intermediate layer** - Query the lakehouse directly!
```

### 3. Time Travel Flow

```
User: SELECT * FROM table FOR SYSTEM_TIME AS OF '2024-01-01'
              ↓
Trino → Iceberg Snapshots → Historical Parquet Files → Results
        (snapshot ID)        (point-in-time)

Iceberg maintains snapshot history, enabling:
- Point-in-time queries
- Rollback capabilities
- Audit and compliance
```

## Component Architecture

### Kafka Cluster (KRaft Mode)

**Containers**: `kafka-1`, `kafka-2`, `kafka-3`

**Role**: Event streaming backbone without Zookeeper dependency

**Configuration**:
```yaml
- Mode: KRaft (Kafka Raft) - self-managing consensus
- Brokers: 3 (all act as both broker and controller)
- Replication: 2x for fault tolerance
- Partitions: Auto-created per topic
- Protocol: PLAINTEXT (SSL/SASL for production)
```

**Why KRaft?**
- Eliminates Zookeeper dependency
- Simpler operations (one less service)
- Faster metadata propagation
- Better scalability
- Industry standard moving forward

### Schema Registry

**Container**: `schema-registry`

**Role**: Schema validation and evolution for Kafka messages

**Features**:
- Avro/JSON schema storage
- Backward/forward compatibility checks
- Schema versioning
- Prevents bad data from entering pipeline

### Data Generator

**Container**: `data-generator`

**Role**: Produces realistic streaming data for testing

**Implementation**:
```python
# Python with Faker library
- Banking transactions (purchases, transfers, withdrawals)
- Realistic merchant names and categories
- Fraud simulation (2% of transactions)
- Configurable generation rate
- Kafka producer with schema validation
```

**Why Custom Generator?**
- Removed Kafka Connect dependency
- More flexible than Datagen connector
- Easier to customize for specific use cases
- Lightweight Python container

### Spark Cluster

**Containers**: `spark-master`, `spark-worker-1`, `spark-worker-2`

**Role**: Real-time stream processing and Iceberg writes

**Architecture**:
- 1 Master node (coordinator)
- 2 Worker nodes (executors)
- All run on same physical server
- Connected via Docker network `data-platform`

**Streaming Job**:
```python
# Continuous Spark Streaming
Kafka Source → DataFrame → Iceberg Sink

- Micro-batch processing (2-second batches)
- Exactly-once semantics
- Automatic checkpointing
- ACID writes to Iceberg
- No manual orchestration needed
```

**Why Continuous Streaming?**
- No Airflow orchestration needed
- Always up-to-date data
- Simpler operations
- Lower latency

### Apache Iceberg

**Storage Format**: Parquet data files + JSON/Avro metadata

**Role**: ACID data lake table format

**Key Features**:

1. **ACID Transactions**
   - Atomic commits
   - Isolation between readers/writers
   - Consistent snapshots
   - Durable writes

2. **Time Travel**
   ```sql
   -- Query as of specific time
   SELECT * FROM table FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 12:00:00';

   -- Query specific snapshot
   SELECT * FROM table FOR SYSTEM_VERSION AS OF 1234567890;

   -- View snapshot history
   SELECT * FROM table$snapshots;
   ```

3. **Schema Evolution**
   - Add columns
   - Drop columns
   - Rename columns
   - Change types
   - No full table rewrite needed

4. **Partition Evolution**
   - Change partitioning without rewriting data
   - Hidden partitioning (automatic)

**Storage Layout**:
```
s3://lakehouse/
├── database/
│   └── table_name/
│       ├── data/
│       │   ├── year=2024/
│       │   │   ├── month=01/
│       │   │   │   └── data-001.parquet
│       │   │   └── month=02/
│       │   │       └── data-002.parquet
│       └── metadata/
│           ├── v1.metadata.json
│           ├── v2.metadata.json
│           └── snap-123.avro
```

### Hive Metastore

**Containers**: `hive-metastore`, `postgres`

**Role**: Catalog management for Iceberg tables

**Stores**:
- Table schemas
- Partition information
- Snapshot pointers
- Table locations in MinIO
- Column statistics

**Why Needed?**
- Trino requires metastore to discover tables
- Industry standard for lakehouse catalogs
- Supports multiple query engines (Trino, Spark, Presto)

**PostgreSQL Backend**:
- Lightweight metadata database
- Only stores metadata (no actual data)
- Credentials: `hive/hive123`

### Trino

**Container**: `trino`

**Role**: Distributed SQL query engine for lakehouse

**Architecture**:
- Coordinator node (query planning, coordination)
- Worker nodes (data processing) - can add more
- MPP (Massively Parallel Processing)
- Pushdown optimizations to storage

**Catalogs**:
```properties
# iceberg.properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore:9083

# minio.properties
connector.name=hive
hive.s3.endpoint=http://minio:9000
```

**Query Capabilities**:
- Full ANSI SQL support
- Joins across catalogs
- Complex aggregations
- Window functions
- CTEs (Common Table Expressions)
- Time travel queries

**Why Trino?**
- Fast SQL on data lakes
- No ETL needed
- Scales to petabytes
- Open source and vendor-neutral
- Active development community

### MinIO

**Container**: `minio`

**Role**: S3-compatible object storage

**Configuration**:
- Single-node mode (PoC)
- Console: http://localhost:9001
- API: http://localhost:9000
- Bucket: `lakehouse`

**Why MinIO?**
- S3-compatible (easy migration to AWS later)
- High performance
- Lightweight for on-prem
- Docker-friendly

### Monitoring Stack

**Prometheus** (`prometheus`)
- Scrapes metrics from all services
- JMX exporters for Kafka/Spark
- Time-series database
- Alerting rules

**Grafana** (`grafana`)
- Visualization dashboards
- Pre-configured datasource to Prometheus
- Custom dashboards for:
  - Kafka metrics (throughput, lag)
  - Spark metrics (jobs, stages)
  - System resources (CPU, memory)
  - MinIO metrics (storage, requests)

**Kafka UI** (`kafka-ui`)
- Web interface for Kafka management
- Topic browsing
- Consumer group monitoring
- Schema registry integration
- Message inspection

## Network Architecture

### Docker Network: `data-platform`

All services communicate via Docker bridge network:

```
Subnet: 172.20.0.0/16

Service DNS Resolution:
- kafka-1.data-platform
- spark-master.data-platform
- trino.data-platform
- hive-metastore.data-platform
- minio.data-platform
- postgres.data-platform
```

### Port Mapping

| Service | Container Port | Host Port | Protocol |
|---------|---------------|-----------|----------|
| Kafka 1 | 29092 | 9092 | Kafka |
| Kafka 2 | 29092 | 9093 | Kafka |
| Kafka 3 | 29092 | 9094 | Kafka |
| Schema Registry | 8081 | 8081 | HTTP |
| Kafka UI | 8080 | 8080 | HTTP |
| Spark Master | 8080 | 8888 | HTTP |
| Spark Worker 1 | 8081 | 8081 | HTTP |
| Spark Worker 2 | 8082 | 8082 | HTTP |
| Trino | 8080 | 8086 | HTTP |
| Hive Metastore | 9083 | 9083 | Thrift |
| PostgreSQL | 5432 | 5432 | PostgreSQL |
| MinIO API | 9000 | 9000 | S3 |
| MinIO Console | 9001 | 9001 | HTTP |
| Prometheus | 9090 | 9090 | HTTP |
| Grafana | 3000 | 3000 | HTTP |

## Data Models

### Kafka Topics

| Topic | Partitions | Replication | Schema | Purpose |
|-------|-----------|-------------|--------|---------|
| `banking-transactions` | 3 | 2 | Avro | Raw transaction events |
| `_schemas` | 1 | 2 | Internal | Schema registry storage |

### Iceberg Tables

**`iceberg.default.transactions`**

```sql
CREATE TABLE iceberg.default.transactions (
    transaction_id VARCHAR,
    timestamp TIMESTAMP,
    account_id VARCHAR,
    customer_id VARCHAR,
    transaction_type VARCHAR,
    amount DECIMAL(18,2),
    currency VARCHAR,
    merchant VARCHAR,
    location STRUCT<
        city VARCHAR,
        state VARCHAR,
        country VARCHAR,
        latitude DOUBLE,
        longitude DOUBLE
    >,
    is_fraud BOOLEAN,
    status VARCHAR,
    metadata MAP<VARCHAR, VARCHAR>
)
PARTITIONED BY (day(timestamp))
LOCATION 's3://lakehouse/default/transactions';
```

**Schema Features**:
- Nested structures (location)
- Complex types (MAP for metadata)
- Partitioned by day for query performance
- Evolves without downtime

## Deployment Models

### Single Server Deployment (PoC)

**Recommended Specs**:
- CPU: 16 cores
- RAM: 24 GB
- Storage: 200 GB SSD
- OS: Ubuntu 20.04+ / CentOS 8+

**All services run as Docker containers on one machine**:
```
Server (Physical/VM)
├── Docker Engine
    ├── kafka-1 (container)
    ├── kafka-2 (container)
    ├── kafka-3 (container)
    ├── spark-master (container)
    ├── spark-worker-1 (container)
    ├── spark-worker-2 (container)
    ├── trino (container)
    ├── hive-metastore (container)
    ├── postgres (container)
    ├── minio (container)
    ├── data-generator (container)
    ├── prometheus (container)
    └── grafana (container)
```

**Benefits**:
- Simple deployment
- No network configuration between machines
- Fast inter-container communication
- Perfect for PoC and development
- Easy to scale later

### Lightweight Mode (12-16GB RAM)

Activate with:
```bash
docker-compose -f docker-compose.yml -f docker-compose.lightweight.yml up -d
```

**Changes**:
- Single Kafka broker (instead of 3)
- Single Spark worker (instead of 2)
- Reduced memory allocations:
  - Spark Worker: 4G → 2G
  - Trino: 2G → 1G
  - Kafka: 1G → 512M

## Scalability Considerations

### Horizontal Scaling

**Kafka**: Add more brokers
```yaml
kafka-4:
  extends: kafka-1
  environment:
    KAFKA_NODE_ID: 4
  ports:
    - "9095:9095"
```

**Spark**: Add more workers
```yaml
spark-worker-3:
  extends: spark-worker-1
  ports:
    - "8083:8083"
```

**Trino**: Add worker nodes (separate coordinator/worker)
```yaml
trino-worker-1:
  image: trinodb/trino
  environment:
    - coordinator=false
```

### Vertical Scaling

Increase container resources in `docker-compose.yml`:
```yaml
spark-worker-1:
  deploy:
    resources:
      limits:
        cpus: '4'
        memory: 8G
```

## Performance Tuning

### Kafka Optimization

```properties
# Increase throughput
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576

# Increase retention
log.retention.hours=168
log.segment.bytes=1073741824
```

### Spark Optimization

```properties
# Increase parallelism
spark.sql.shuffle.partitions=200
spark.default.parallelism=16

# Memory tuning
spark.executor.memory=4g
spark.driver.memory=2g
spark.memory.fraction=0.8
```

### Trino Optimization

```properties
# Memory allocation
query.max-memory=4GB
query.max-memory-per-node=2GB
query.max-total-memory-per-node=3GB

# Concurrency
task.concurrency=16
task.max-worker-threads=64
```

### Iceberg Optimization

```python
# Spark write options
.option("write.parquet.compression-codec", "snappy")
.option("write.metadata.compression-codec", "gzip")
.option("write.target-file-size-bytes", "134217728")  # 128MB
```

## Security Architecture

### Authentication

**Current (PoC)**:
- Basic password auth for UIs
- No inter-service authentication

**Production Recommendations**:
- SASL/SCRAM for Kafka
- LDAP/OAuth for Trino
- IAM-style policies for MinIO
- SSL/TLS everywhere

### Network Security

**Current**:
- Isolated Docker network
- Port exposure only where needed

**Production**:
- Firewall rules
- VPN for external access
- Network segmentation
- Encryption in transit

### Data Security

**Current**:
- Passwords in `.env` file
- Plaintext communication

**Production**:
- Secrets management (Vault)
- Encryption at rest (MinIO KMS)
- Column-level encryption
- Row-level security in Trino

## Monitoring & Observability

### Metrics Collection

**Prometheus Scrape Config**:
```yaml
scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka-1:9101', 'kafka-2:9102', 'kafka-3:9103']

  - job_name: 'spark'
    static_configs:
      - targets: ['spark-master:8080', 'spark-worker-1:8081']

  - job_name: 'minio'
    static_configs:
      - targets: ['minio:9000']
```

### Key Metrics

**Kafka**:
- Messages in/out per second
- Consumer lag
- Replica sync status
- Disk usage

**Spark**:
- Active jobs
- Executor status
- Task duration
- Memory usage

**Trino**:
- Query rate
- Query duration
- Worker status
- Memory usage

**System**:
- CPU utilization
- Memory usage
- Disk I/O
- Network bandwidth

### Logging

**Log Aggregation**:
```bash
# View all logs
docker-compose logs -f

# Service-specific
docker-compose logs -f kafka-1
docker-compose logs -f spark-master
docker-compose logs -f trino
```

**Log Levels**:
- Kafka: WARN (reduce noise)
- Spark: INFO
- Trino: INFO
- Hive Metastore: WARN

## Backup & Recovery

### What to Backup

1. **Hive Metastore (PostgreSQL)**
   ```bash
   make backup-postgres
   # Creates: backup/postgres_backup_YYYYMMDD_HHMMSS.sql
   ```

2. **Iceberg Data (MinIO)**
   ```bash
   make backup-minio
   # Mirrors: lakehouse bucket to local backup
   ```

3. **Configuration Files**
   - docker-compose.yml
   - .env
   - trino/catalog/*.properties
   - prometheus.yml

### Recovery Procedures

**Database Recovery**:
```bash
# Restore PostgreSQL
docker exec -i postgres psql -U hive -d metastore < backup.sql
```

**Data Recovery**:
```bash
# Restore MinIO bucket
docker exec minio-client mc mirror backup/lakehouse myminio/lakehouse
```

## Comparison: Traditional vs Lakehouse

| Aspect | Traditional Architecture | This Lakehouse |
|--------|-------------------------|----------------|
| **Components** | 14 services | 10 services |
| **Orchestration** | Airflow required | None (continuous streaming) |
| **Data Warehouse** | Separate (PostgreSQL) | Not needed |
| **Query Layer** | Superset on PostgreSQL | Trino on Iceberg |
| **ETL** | Lake → Warehouse | Not needed |
| **Coordination** | Zookeeper | KRaft (built-in) |
| **Complexity** | High | Medium |
| **Latency** | Higher (batch ETL) | Lower (continuous) |
| **Storage Cost** | 2x (Lake + Warehouse) | 1x (Lakehouse only) |
| **ACID** | Warehouse only | Everywhere (Iceberg) |
| **Time Travel** | Not available | Built-in |
| **Ops Overhead** | High | Low |

## Technology Decisions

### Why Kafka in KRaft Mode?
- Eliminates Zookeeper complexity
- Faster metadata propagation
- Easier operations
- Industry direction

### Why No Airflow?
- Continuous streaming doesn't need batch orchestration
- Simpler operations
- Lower latency
- Fewer components to manage

### Why Trino Instead of Superset + PostgreSQL?
- Query data lake directly (no ETL)
- Better performance on large datasets
- Standard SQL interface
- No data duplication

### Why Iceberg?
- ACID transactions on data lake
- Time travel and versioning
- Schema evolution
- Open format (no vendor lock-in)

### Why Custom Data Generator Instead of Kafka Connect?
- More flexible and customizable
- Lightweight Python container
- Easier to modify for specific use cases
- One less service to manage

## Migration Path

### From This PoC to Production

1. **Move to Cloud** (Optional)
   - Replace MinIO with S3/GCS/Azure Blob
   - Use managed Kafka (Confluent Cloud, MSK)
   - Managed Trino (Starburst, Ahana)

2. **Add Authentication**
   - SASL for Kafka
   - LDAP/OAuth for Trino
   - IAM for object storage

3. **Enable Encryption**
   - SSL/TLS for all services
   - At-rest encryption in storage
   - Secrets management

4. **Scale Horizontally**
   - Multi-node Kafka cluster
   - More Spark workers
   - Trino worker pool

5. **Add Governance**
   - Enable DataHub
   - Implement data quality checks
   - Audit logging

## Troubleshooting Guide

### Kafka Issues
**Problem**: Brokers not forming cluster
```bash
# Check logs
docker-compose logs kafka-1

# Verify cluster ID is same across brokers
docker exec kafka-1 cat /var/lib/kafka/data/meta.properties
```

### Spark Issues
**Problem**: Streaming job not running
```bash
# Check Spark UI
open http://localhost:8888

# Verify Kafka connectivity
docker exec spark-master curl http://kafka-1:29092
```

### Trino Issues
**Problem**: Cannot query Iceberg tables
```bash
# Verify Hive Metastore
docker-compose logs hive-metastore

# Check catalog config
docker exec trino cat /etc/trino/catalog/iceberg.properties

# Test metastore connection
docker exec trino trino --execute "SHOW TABLES IN iceberg.default"
```

## References

- Apache Kafka: https://kafka.apache.org/
- Apache Spark: https://spark.apache.org/
- Apache Iceberg: https://iceberg.apache.org/
- Trino: https://trino.io/
- Hive Metastore: https://hive.apache.org/
- MinIO: https://min.io/

## Related Documentation

### Guides & Tutorials
- **[Beginner Walkthrough](guides/data-ingestion-processing/docs/BEGINNER_WALKTHROUGH.md)** - Step-by-step guide for first-time users
- **[Data Streaming Guide](guides/data-ingestion-processing/docs/DATA_STREAMING_GUIDE.md)** - Complete streaming and processing documentation
- **[Guides Index](guides/data-ingestion-processing/README.md)** - Central hub for all guides and examples

### Quick References
- [README.md](README.md) - Platform overview and quick start
- [QUICK_START.md](QUICK_START.md) - Fast setup guide
- [Automation Scripts](guides/data-ingestion-processing/scripts/) - Pipeline automation tools

---

**This architecture provides a modern, simplified data lakehouse that can run on a single server while maintaining production-ready capabilities for analytics and data engineering workloads.**
