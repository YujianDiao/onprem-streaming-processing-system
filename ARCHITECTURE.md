# On-Premise Data Streaming, Processing & Serving System - Architecture Design

## Executive Summary

This document outlines a comprehensive on-premise data streaming and processing platform designed for proof-of-concept deployments. The system leverages modern data engineering tools to provide real-time and batch processing capabilities with strong data governance.

## System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA INGESTION LAYER                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌──────────────────┐      ┌─────────────────┐      ┌──────────────────┐   │
│  │ Kafka Datagen    │─────▶│  Apache Kafka   │◀────▶│ Schema Registry  │   │
│  │ Connector        │      │  (Event Stream) │      │  (Avro/JSON)     │   │
│  └──────────────────┘      └────────┬────────┘      └──────────────────┘   │
│                                     │                                        │
│                            ┌────────▼────────┐                              │
│                            │   Zookeeper     │                              │
│                            │  (Coordination) │                              │
│                            └─────────────────┘                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     │ Stream Events
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PROCESSING LAYER                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      Apache Spark Cluster                            │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │   │
│  │  │ Spark Master │  │ Spark Worker │  │ Spark Worker │              │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │   │
│  │                                                                       │   │
│  │  • Spark Streaming (Kafka → Processing → Storage)                   │   │
│  │  • Batch Processing (Iceberg Tables)                                │   │
│  │  • Data Transformation & Aggregation                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      Apache Airflow                                  │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │   │
│  │  │   Webserver  │  │   Scheduler  │  │    Worker    │              │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │   │
│  │                                                                       │   │
│  │  • Workflow Orchestration                                            │   │
│  │  • ETL Pipeline Management                                           │   │
│  │  • Job Scheduling & Monitoring                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     │ Processed Data
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          STORAGE LAYER                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │                    MinIO (Object Storage)                          │     │
│  │                                                                     │     │
│  │  ┌──────────────────────────────────────────────────────────┐    │     │
│  │  │         Apache Iceberg Tables (Data Lake)                │    │     │
│  │  │                                                            │    │     │
│  │  │  • Raw Data (Bronze Layer)                               │    │     │
│  │  │  • Cleaned Data (Silver Layer)                           │    │     │
│  │  │  • Aggregated Data (Gold Layer)                          │    │     │
│  │  │  • ACID Transactions, Time Travel, Schema Evolution     │    │     │
│  │  └──────────────────────────────────────────────────────────┘    │     │
│  └───────────────────────────────────────────────────────────────────┘     │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │                    PostgreSQL Database                             │     │
│  │                                                                     │     │
│  │  • Serving Layer (Optimized for Queries)                          │     │
│  │  • Materialized Views                                              │     │
│  │  • Application Database                                            │     │
│  └───────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     │ Metadata & Serving
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    METADATA & MONITORING LAYER                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐          │
│  │    DataHub       │  │   Kafka UI       │  │    Prometheus    │          │
│  │  (Data Catalog)  │  │  (Kafka Monitor) │  │    (Metrics)     │          │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘          │
│                                                                               │
│  ┌──────────────────┐  ┌──────────────────┐                                 │
│  │    Grafana       │  │    Superset      │                                 │
│  │  (Dashboards)    │  │  (BI & Analytics)│                                 │
│  └──────────────────┘  └──────────────────┘                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Patterns

### Pattern 1: Real-Time Streaming Pipeline

```
Kafka Datagen → Kafka Topics → Spark Streaming → MinIO (Iceberg) → PostgreSQL → Superset
                      │                                    │
                      └──────────────────────────────────────▶ DataHub (Metadata)
```

1. **Data Generation**: Kafka Datagen produces synthetic banking data (transactions, accounts, etc.)
2. **Event Streaming**: Events published to Kafka topics with schema validation via Schema Registry
3. **Stream Processing**: Spark Structured Streaming consumes events, applies transformations
4. **Lake Storage**: Processed data written to Iceberg tables in MinIO (Bronze → Silver → Gold)
5. **Serving**: Aggregated data loaded into PostgreSQL for low-latency queries
6. **Visualization**: Superset queries PostgreSQL and Iceberg for dashboards
7. **Governance**: DataHub catalogs all datasets, lineage, and schemas

### Pattern 2: Batch Processing Pipeline

```
Airflow DAG → Spark Batch Jobs → Read from Iceberg → Transform → Write to Iceberg/PostgreSQL
                                                                              │
                                                                              ▼
                                                                          Superset
```

1. **Orchestration**: Airflow schedules daily/hourly batch jobs
2. **Processing**: Spark reads from Iceberg tables, performs complex transformations
3. **Storage**: Results written back to Iceberg (Silver/Gold layers) and PostgreSQL
4. **Reporting**: Business reports and dashboards updated in Superset

## Component Details

### 1. Apache Kafka Cluster
- **Purpose**: Event streaming backbone
- **Configuration**:
  - 3 brokers for PoC (scalable to more)
  - Replication factor: 2-3
  - Retention: 7 days for hot topics
- **Topics**:
  - `banking.transactions.raw` - Raw transaction events
  - `banking.accounts.cdc` - Account change data capture
  - `banking.customers.enriched` - Enriched customer profiles

### 2. Apache Zookeeper
- **Purpose**: Kafka coordination and metadata management
- **Configuration**: 3-node ensemble for PoC
- **Note**: Consider KRaft mode for future versions

### 3. Schema Registry (Confluent)
- **Purpose**: Centralized schema management for Kafka
- **Format**: Avro, JSON Schema, Protobuf support
- **Benefits**:
  - Schema evolution and compatibility checking
  - Prevents schema drift
  - Reduces message size with schema references

### 4. Apache Spark Cluster
- **Purpose**: Unified batch and streaming processing
- **Configuration**:
  - 1 Master node
  - 2-3 Worker nodes
  - Memory: 4-8GB per worker for PoC
- **Jobs**:
  - Spark Structured Streaming for real-time
  - Spark SQL for batch analytics
  - Integration with Iceberg catalog

### 5. Apache Airflow
- **Purpose**: Workflow orchestration
- **Components**:
  - Webserver (UI)
  - Scheduler (DAG execution)
  - Workers (task execution)
  - PostgreSQL (metadata DB)
- **Use Cases**:
  - Daily batch aggregations
  - Data quality checks
  - Model training pipelines
  - Data reconciliation

### 6. MinIO Object Storage
- **Purpose**: S3-compatible data lake storage
- **Configuration**:
  - Distributed mode with 4+ drives recommended
  - Standalone mode acceptable for PoC
- **Buckets**:
  - `lakehouse` - Iceberg tables
  - `raw-data` - Landing zone
  - `backups` - System backups

### 7. Apache Iceberg
- **Purpose**: Table format for data lake
- **Benefits**:
  - ACID transactions
  - Time travel queries
  - Schema evolution
  - Partition evolution
  - Hidden partitioning
- **Catalog**: Hadoop catalog or Hive Metastore
- **Table Organization**:
  - Bronze: Raw, immutable data
  - Silver: Cleaned, validated data
  - Gold: Business-level aggregates

### 8. PostgreSQL
- **Purpose**: Serving layer and operational data store
- **Configuration**:
  - Connection pooling (PgBouncer)
  - Read replicas for scaling
- **Schemas**:
  - `serving` - Optimized tables for applications
  - `airflow` - Airflow metadata
  - `superset` - Superset metadata

### 9. DataHub
- **Purpose**: Data catalog and governance
- **Features**:
  - Data discovery
  - Lineage tracking
  - Schema registry
  - Data quality metrics
  - Access control
- **Components**:
  - GMS (Generalized Metadata Service)
  - Frontend
  - Elasticsearch (search)
  - MySQL (storage)

### 10. Monitoring & Visualization

#### Kafka UI (Kafdrop/Redpanda Console)
- Topic browsing and message inspection
- Consumer lag monitoring
- Schema registry integration

#### Prometheus
- Metrics collection from all services
- Custom exporters for Kafka, Spark, etc.

#### Grafana
- Real-time dashboards
- Alerting
- System health monitoring

#### Apache Superset
- Business intelligence and data visualization
- SQL Lab for ad-hoc queries
- Dashboard sharing

## Network Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Docker Network: data-platform            │
│                                                               │
│  Ingestion Network (10.0.1.0/24)                            │
│    - Kafka: 10.0.1.10:9092                                  │
│    - Zookeeper: 10.0.1.11:2181                              │
│    - Schema Registry: 10.0.1.12:8081                        │
│                                                               │
│  Processing Network (10.0.2.0/24)                           │
│    - Spark Master: 10.0.2.10:7077                           │
│    - Spark Workers: 10.0.2.11-13                            │
│    - Airflow: 10.0.2.20:8080                                │
│                                                               │
│  Storage Network (10.0.3.0/24)                              │
│    - MinIO: 10.0.3.10:9000                                  │
│    - PostgreSQL: 10.0.3.11:5432                             │
│                                                               │
│  Metadata Network (10.0.4.0/24)                             │
│    - DataHub: 10.0.4.10:9002                                │
│    - Prometheus: 10.0.4.20:9090                             │
│    - Grafana: 10.0.4.21:3000                                │
│    - Superset: 10.0.4.22:8088                               │
│    - Kafka UI: 10.0.4.23:8080                               │
└──────────────────────────────────────────────────────────────┘
```

## Data Security & Governance

### Authentication & Authorization
- **Kafka**: SASL/SCRAM or mTLS for PoC
- **PostgreSQL**: Password authentication with SSL
- **MinIO**: Access keys and bucket policies
- **DataHub**: OIDC/LDAP integration ready

### Data Encryption
- **At Rest**: MinIO encryption, PostgreSQL TLS
- **In Transit**: SSL/TLS for all services
- **Secrets**: Docker secrets or external vault

### Data Quality
- **Great Expectations**: Data validation framework
- **dbt**: Data transformation testing
- **Custom Spark Jobs**: Automated quality checks

## Scalability Considerations

### Horizontal Scaling
- **Kafka**: Add brokers, increase partitions
- **Spark**: Add worker nodes
- **MinIO**: Add nodes to distributed cluster
- **PostgreSQL**: Read replicas, sharding

### Vertical Scaling
- Increase container resources
- Optimize JVM settings for Spark/Kafka
- Tune PostgreSQL parameters

## Deployment Strategy

### Infrastructure as Code
- Docker Compose for single-node PoC
- Kubernetes manifests for production-ready deployment
- Terraform for cloud infrastructure (if needed)

### CI/CD Pipeline
- Git for version control
- Automated testing for Spark jobs and Airflow DAGs
- Container image building and scanning
- Deployment automation

## Cost Optimization for PoC

### Resource Requirements (Minimum)
- **CPU**: 16 cores (24+ recommended)
- **Memory**: 32 GB RAM (64+ recommended)
- **Storage**: 500 GB SSD (1TB+ for longer retention)
- **Network**: 1 Gbps

### Container Resource Limits
| Service | CPU | Memory | Notes |
|---------|-----|--------|-------|
| Kafka (3 brokers) | 6 cores | 6 GB | 2GB per broker |
| Spark (1+2 nodes) | 6 cores | 12 GB | 4GB per worker |
| Airflow (3 services) | 3 cores | 4 GB | Lightweight for PoC |
| PostgreSQL | 2 cores | 4 GB | Can be shared |
| MinIO | 2 cores | 2 GB | Increase for distributed |
| DataHub | 4 cores | 6 GB | Resource intensive |
| Others | 3 cores | 4 GB | Monitoring stack |

**Total**: ~26 cores, ~38 GB RAM

## Alternative Configurations

### Lightweight PoC (8 cores, 16 GB RAM)
- Remove: DataHub, reduce Kafka to 1 broker
- Use: Kafka UI, simple Airflow setup
- Single Spark worker

### Production-Ready Configuration
- Add: High availability for all services
- Separate: Storage and compute infrastructure
- Implement: Full security, monitoring, backups

## Technology Alternatives Considered

| Current Choice | Alternative | Reason for Selection |
|----------------|-------------|---------------------|
| Kafka | Pulsar, RabbitMQ | Industry standard, ecosystem |
| Spark | Flink, Storm | Unified batch/stream, maturity |
| Iceberg | Delta Lake, Hudi | Open governance, feature set |
| Airflow | Prefect, Dagster | Maturity, community support |
| DataHub | Apache Atlas, Amundsen | Modern UI, active development |
| MinIO | SeaweedFS, Ceph | S3 compatibility, ease of use |

## Migration Path

### Phase 1: Core Infrastructure (Week 1-2)
- Deploy Kafka, Zookeeper, Schema Registry
- Setup MinIO and PostgreSQL
- Basic monitoring with Kafka UI

### Phase 2: Processing Layer (Week 3-4)
- Deploy Spark cluster
- Setup Airflow
- Implement first Spark streaming job

### Phase 3: Data Lake (Week 5-6)
- Configure Iceberg catalog
- Implement medallion architecture (Bronze/Silver/Gold)
- Setup data quality checks

### Phase 4: Governance & Analytics (Week 7-8)
- Deploy DataHub
- Setup Superset dashboards
- Implement lineage tracking

### Phase 5: Optimization (Week 9+)
- Performance tuning
- Advanced monitoring with Prometheus/Grafana
- Documentation and training

## Success Metrics

1. **Performance**: 10K+ events/sec throughput
2. **Latency**: <5 seconds end-to-end for streaming
3. **Availability**: 99%+ uptime for PoC
4. **Data Quality**: 99.9%+ accuracy in processing
5. **Time to Insight**: <1 hour for new dashboard creation

## References & Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [DataHub Documentation](https://datahubproject.io/docs/)
- [MinIO Documentation](https://min.io/docs/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

## Next Steps

1. Review and approve architecture
2. Setup Docker Compose environment
3. Configure network and security
4. Deploy services incrementally
5. Implement sample data pipelines
6. Create monitoring dashboards
7. Document operational procedures
