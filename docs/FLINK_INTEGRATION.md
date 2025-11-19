# Apache Flink Integration for Banking Data Processing

## Overview

This document describes the Apache Flink integration added to the on-premise streaming processing system. Flink complements the existing Spark Structured Streaming setup by providing **true real-time stream processing** with sub-second latency for banking use cases.

## Architecture

### Hybrid Streaming Architecture

The system now supports both **Spark** and **Flink** for different streaming requirements:

```
                                    ┌─────────────────────────────┐
                                    │    Kafka Cluster (KRaft)     │
                                    │   banking.transactions.raw   │
                                    └──────────┬──────────────────┘
                                              │
                          ┌───────────────────┴───────────────────┐
                          │                                       │
                          ▼                                       ▼
                ┌──────────────────┐                  ┌──────────────────┐
                │  Spark Streaming │                  │  Flink Streaming │
                │  (Near Real-time) │                  │  (True Real-time) │
                │  30s micro-batch │                  │  Event-by-event  │
                └──────────────────┘                  └──────────────────┘
                          │                                       │
                          ▼                                       ▼
                ┌──────────────────┐                  ┌──────────────────┐
                │  Iceberg Tables  │                  │   Kafka Topics   │
                │  (Lakehouse)     │                  │   + Iceberg      │
                └──────────────────┘                  └──────────────────┘
                          │                                       │
                          └───────────────────┬───────────────────┘
                                              ▼
                                    ┌──────────────────┐
                                    │  Trino (Analytics)│
                                    │  MinIO (Storage)  │
                                    └──────────────────┘
```

### When to Use Spark vs Flink

| Requirement | Use | Reason |
|------------|-----|--------|
| Data Lake ingestion | **Spark** | Better Iceberg integration, batch optimization |
| Complex fraud detection | **Flink** | CEP capabilities, sub-second detection |
| Real-time balance updates | **Flink** | Stateful processing, low latency |
| Historical analytics | **Spark** | Mature SQL, better for batch operations |
| Transaction monitoring | **Flink** | Windowing, alerting, millisecond latency |
| ETL to lakehouse | **Spark** | Optimized for large-scale writes |

## Flink Cluster Components

### JobManager (1 instance)
- **Role**: Cluster coordinator, schedules tasks, manages checkpoints
- **Resources**: 2GB memory
- **Port**: 8081 (Web UI), 6123 (RPC)

### TaskManager (2 instances)
- **Role**: Execute tasks, manage state, process data
- **Resources**: 4GB memory, 4 task slots each
- **Total Capacity**: 8 parallel tasks

### State Backend
- **Type**: RocksDB (optimized for large state)
- **Checkpoints**: MinIO (s3://lakehouse/flink-checkpoints)
- **Savepoints**: MinIO (s3://lakehouse/flink-savepoints)
- **Interval**: 30 seconds
- **Mode**: EXACTLY_ONCE

## Flink Jobs for Banking

### 1. Fraud Detection Job

**Purpose**: Real-time fraud detection using Complex Event Processing (CEP)

**Features**:
- Rule-based fraud detection (high-value transactions >$10k)
- Velocity checks (multiple transactions in short time)
- CEP pattern matching (failed attempts → success pattern)
- Geographic anomaly detection (transactions from different cities)

**Input**: `banking.transactions.raw`

**Output**: `banking.fraud.alerts`

**Alert Types**:
- `RULE_BASED`: High-value or flagged transactions
- `VELOCITY_CHECK`: >5 transactions in 1 minute
- `CEP_PATTERN`: 3 failed attempts + 1 success in 5 minutes
- `GEOGRAPHIC_ANOMALY`: Transactions from multiple cities in 5 minutes

**Example Output**:
```json
{
  "transactionId": "uuid-123",
  "accountId": "ACC1234",
  "fraudType": "VELOCITY_CHECK",
  "reason": "8 transactions in 1 minute (total: $45,230.00)",
  "severity": "HIGH",
  "amount": 45230.00,
  "detectedAt": 1699999999999
}
```

### 2. Transaction Monitoring Job

**Purpose**: Real-time transaction monitoring and alerting

**Features**:
- Large transaction alerts (>$50k)
- Transaction volume monitoring (hourly sliding windows)
- Daily limit tracking (stateful processing)
- Transaction pattern analysis

**Input**: `banking.transactions.raw`

**Outputs**:
- `banking.monitoring.alerts`: Transaction alerts
- `banking.metrics.realtime`: Real-time banking KPIs

**Alert Types**:
- `LARGE_TRANSACTION`: Transactions ≥ $50,000
- `HIGH_VOLUME`: >20 transactions per hour
- `DAILY_LIMIT_EXCEEDED`: Account exceeds $100k/day
- `DAILY_LIMIT_WARNING`: Account reaches 80% of limit
- `UNUSUAL_PATTERN`: 10+ consecutive same-type transactions

**Metrics Computed** (5-minute sliding windows):
- Total transaction count
- Total transaction volume
- Average transaction amount
- Max transaction amount
- Fraud rate (%)

### 3. Balance Aggregation Job

**Purpose**: Real-time account balance calculation and tracking

**Features**:
- Running balance calculation (stateful)
- Hourly balance snapshots
- Low balance alerts (<$100)
- Overdraft detection (balance < $0)
- Customer-level aggregation

**Input**: `banking.transactions.raw`

**Outputs**:
- `banking.balances.realtime`: Current account balances
- `banking.balance.alerts`: Balance alerts
- `banking.balance.snapshots`: Hourly snapshots

**Balance Calculation Logic**:
- `DEPOSIT`: Credit (+amount)
- `WITHDRAWAL`, `PURCHASE`, `PAYMENT`: Debit (-amount)
- `TRANSFER`: Debit (-amount) for sender

**Example Balance Output**:
```json
{
  "accountId": "ACC1234",
  "currentBalance": 5432.10,
  "availableBalance": 5432.10,
  "lastTransactionTime": 1699999999999,
  "lastTransactionId": "uuid-456",
  "transactionCount": 127
}
```

## Setup and Deployment

### Prerequisites

1. Docker and Docker Compose installed
2. Sufficient resources (16GB RAM recommended)
3. Kafka cluster running
4. MinIO and Hive Metastore available

### Initial Setup

1. **Start Flink Cluster**:
```bash
# Start Flink JobManager and TaskManagers
docker-compose up -d flink-jobmanager flink-taskmanager-1 flink-taskmanager-2

# Verify Flink cluster is healthy
docker-compose ps | grep flink
```

2. **Create Kafka Topics**:
```bash
# Create all necessary topics for Flink outputs
bash scripts/setup-flink-topics.sh
```

3. **Build and Submit Jobs**:
```bash
# Build and submit all jobs
cd flink
./build-and-submit.sh all

# Or submit individual jobs
./build-and-submit.sh fraud-detection
./build-and-submit.sh transaction-monitoring
./build-and-submit.sh balance-aggregation
```

### Verify Deployment

1. **Check Flink Web UI**: http://localhost:8081
   - Verify JobManager is running
   - Check TaskManager slots (should show 8 available)
   - View submitted jobs

2. **Monitor Job Status**:
```bash
# List running jobs
docker exec flink-jobmanager /opt/flink/bin/flink list -r

# View job logs
docker-compose logs -f flink-taskmanager-1
```

3. **Check Kafka Topics**:
```bash
# Monitor fraud alerts
docker exec kafka-1 kafka-console-consumer \
    --bootstrap-server kafka-1:29092 \
    --topic banking.fraud.alerts \
    --from-beginning

# Monitor real-time balances
docker exec kafka-1 kafka-console-consumer \
    --bootstrap-server kafka-1:29092 \
    --topic banking.balances.realtime \
    --from-beginning
```

## Managing Flink Jobs

### Canceling Jobs

```bash
# List running jobs to get job ID
docker exec flink-jobmanager /opt/flink/bin/flink list -r

# Cancel a specific job
docker exec flink-jobmanager /opt/flink/bin/flink cancel <JOB_ID>

# Cancel with savepoint
docker exec flink-jobmanager /opt/flink/bin/flink cancel -s <SAVEPOINT_DIR> <JOB_ID>
```

### Creating Savepoints

```bash
# Create savepoint for a running job
docker exec flink-jobmanager /opt/flink/bin/flink savepoint <JOB_ID>
```

### Restoring from Savepoint

```bash
# Submit job from savepoint
docker exec flink-jobmanager /opt/flink/bin/flink run \
    -s <SAVEPOINT_PATH> \
    -c com.banking.flink.jobs.FraudDetectionJob \
    /opt/flink-jobs/target/flink-banking-pipelines-1.0.0.jar
```

## Monitoring and Metrics

### Flink Web UI (http://localhost:8081)

- **Overview**: Cluster status, TaskManager health
- **Jobs**: Running/completed jobs, job graph
- **Task Managers**: Resource utilization, slots
- **Job Manager**: Configuration, logs
- **Checkpoints**: Checkpoint history, state size

### Prometheus Metrics

Flink exposes metrics on port 9249:
```bash
curl http://localhost:9249/metrics
```

**Key Metrics**:
- `flink_taskmanager_Status_JVM_Memory_Heap_Used`: Heap memory
- `flink_jobmanager_numRunningJobs`: Running jobs
- `flink_taskmanager_job_task_numRecordsIn`: Records processed
- `flink_taskmanager_job_task_numRecordsOut`: Records emitted
- `flink_jobmanager_job_lastCheckpointSize`: Checkpoint size

### Logs

```bash
# Flink JobManager logs
docker-compose logs -f flink-jobmanager

# Flink TaskManager logs
docker-compose logs -f flink-taskmanager-1 flink-taskmanager-2

# Follow all Flink logs
docker-compose logs -f flink-jobmanager flink-taskmanager-1 flink-taskmanager-2
```

## Performance Tuning

### Checkpointing

Adjust checkpoint interval in `flink/conf/flink-conf.yaml`:
```yaml
execution.checkpointing.interval: 30000  # 30 seconds (default)
execution.checkpointing.timeout: 600000  # 10 minutes
```

### Parallelism

Modify parallelism per job:
```bash
docker exec flink-jobmanager /opt/flink/bin/flink run \
    -p 4 \  # Set parallelism to 4
    -c com.banking.flink.jobs.FraudDetectionJob \
    /opt/flink-jobs/target/flink-banking-pipelines-1.0.0.jar
```

Or set default parallelism in `flink-conf.yaml`:
```yaml
parallelism.default: 4
```

### Memory Configuration

Adjust TaskManager memory:
```yaml
# In flink-conf.yaml
taskmanager.memory.process.size: 8192m  # 8GB
taskmanager.memory.managed.fraction: 0.4  # 40% for state backend
```

### Task Slots

Increase slots per TaskManager:
```yaml
# In flink-conf.yaml
taskmanager.numberOfTaskSlots: 8  # More slots for higher parallelism
```

## Troubleshooting

### Job Fails to Start

**Issue**: Job submitted but fails immediately

**Solutions**:
1. Check logs: `docker-compose logs flink-taskmanager-1`
2. Verify Kafka connectivity: `docker exec flink-jobmanager ping kafka-1`
3. Check resource availability in Flink UI
4. Ensure JAR is built correctly: `docker exec flink-jobmanager ls -l /opt/flink-jobs/target/`

### Out of Memory Errors

**Issue**: TaskManager crashes with OOM

**Solutions**:
1. Increase TaskManager memory in `docker-compose.yml`
2. Enable incremental checkpoints: `state.backend.incremental: true`
3. Reduce state size by using TTL on state
4. Increase checkpoint interval to reduce overhead

### Checkpoint Failures

**Issue**: Checkpoints timing out or failing

**Solutions**:
1. Check MinIO connectivity: `docker exec flink-taskmanager-1 curl http://minio:9000/minio/health/live`
2. Verify S3 credentials in `flink-conf.yaml`
3. Increase checkpoint timeout in configuration
4. Check state size in Flink UI

### Kafka Connection Issues

**Issue**: Cannot connect to Kafka brokers

**Solutions**:
1. Verify Kafka is running: `docker-compose ps kafka-1`
2. Check network connectivity: `docker exec flink-taskmanager-1 nc -zv kafka-1 29092`
3. Verify bootstrap servers in job code
4. Check Kafka topic exists: `docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092`

## Integration with Existing Components

### Reading Flink Outputs in Trino

Flink writes to both Kafka topics and Iceberg tables. Query results in Trino:

```sql
-- Query fraud alerts (if written to Iceberg)
SELECT * FROM iceberg.default.fraud_alerts
WHERE detected_at > current_timestamp - INTERVAL '1' HOUR
ORDER BY severity DESC;

-- Query real-time balances
SELECT * FROM iceberg.default.account_balances
WHERE current_balance < 100.0
ORDER BY last_transaction_time DESC;
```

### Consuming Flink Outputs in Spark

Read Flink outputs from Kafka in Spark:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ConsumeFraudAlerts").getOrCreate()

fraud_alerts = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:29092") \
    .option("subscribe", "banking.fraud.alerts") \
    .load()

# Process alerts
fraud_alerts.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()
```

### Grafana Dashboards

Create dashboards using Prometheus metrics:
1. Add Prometheus datasource: http://prometheus:9090
2. Import Flink dashboard templates
3. Monitor job metrics, throughput, latency

## Best Practices

### 1. Job Development

- Use event time instead of processing time
- Set appropriate watermark delays (5-10 seconds)
- Enable checkpointing for fault tolerance
- Use keyed state for scalability
- Test with sample data before production

### 2. State Management

- Use TTL for state that doesn't need to be kept forever
- Choose appropriate state backend (RocksDB for large state)
- Monitor state size via checkpoints
- Plan for state migration/evolution

### 3. Kafka Integration

- Use appropriate consumer groups
- Set proper offset reset strategy
- Monitor consumer lag
- Use Kafka transactions for exactly-once semantics

### 4. Resource Planning

- Allocate enough task slots for parallelism
- Monitor JVM heap usage
- Use managed memory for state backend
- Plan for scaling based on throughput

### 5. Monitoring

- Set up alerts on checkpoint failures
- Monitor job restarts
- Track processing latency
- Watch for backpressure

## Example Use Cases

### Real-time Fraud Detection Flow

```
1. Transaction arrives in Kafka
   ↓
2. Flink Fraud Detection Job processes event
   ↓
3. CEP engine checks for patterns
   ↓
4. If fraud detected, emit alert to banking.fraud.alerts
   ↓
5. Downstream systems:
   - Block transaction in real-time
   - Send SMS/email alert to customer
   - Create case in fraud management system
   ↓
6. Alert also written to Iceberg for analysis
   ↓
7. Trino used for fraud pattern analysis
```

### Real-time Balance Updates Flow

```
1. Transaction posted (DEPOSIT/WITHDRAWAL)
   ↓
2. Flink Balance Aggregation Job updates running balance
   ↓
3. New balance emitted to banking.balances.realtime
   ↓
4. If balance < $100, emit low balance alert
   ↓
5. Mobile app receives real-time balance update
   ↓
6. Hourly snapshots written to Iceberg
   ↓
7. Historical balance trends available in Trino
```

## Comparison: Spark vs Flink for Banking

| Feature | Spark Streaming | Flink |
|---------|----------------|-------|
| Latency | 30 seconds (micro-batch) | <1 second (event-by-event) |
| State Management | Limited | Rich (RocksDB, checkpoints) |
| CEP Support | Limited | Native (FlinkCEP) |
| Exactly-Once | Yes (micro-batch) | Yes (distributed snapshots) |
| Iceberg Integration | Excellent | Good |
| SQL Support | Excellent | Good |
| Windowing | Good | Excellent |
| Best For | ETL, batch, analytics | Real-time alerts, CEP |

## Next Steps

1. **Production Hardening**:
   - Enable HA for JobManager
   - Set up monitoring alerts
   - Configure security (Kerberos, SSL)
   - Implement proper logging

2. **Advanced Features**:
   - Add ML-based fraud detection
   - Implement risk scoring models
   - Add customer 360 aggregations
   - Create real-time dashboards

3. **Scaling**:
   - Add more TaskManagers
   - Optimize checkpoint intervals
   - Implement job autoscaling
   - Tune parallelism

## References

- [Apache Flink Documentation](https://flink.apache.org/docs/stable/)
- [Flink Kafka Connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)
- [Flink CEP](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/)
- [Flink Iceberg Connector](https://iceberg.apache.org/docs/latest/flink/)
- [Flink State Backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)
