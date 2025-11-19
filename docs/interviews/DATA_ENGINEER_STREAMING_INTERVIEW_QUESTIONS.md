# Data Engineer (Streaming) - Interview Questions & Answers

**Role:** Data Engineer (Streaming/Real-Time)
**Platform:** On-Premise Streaming Data Lakehouse
**Date:** 2025-11-19

---

## Table of Contents

1. [Streaming Fundamentals](#streaming-fundamentals)
2. [Kafka Architecture & Operations](#kafka-architecture--operations)
3. [Spark Structured Streaming](#spark-structured-streaming)
4. [Stream Processing Patterns](#stream-processing-patterns)
5. [Performance & Optimization](#performance--optimization)
6. [Fault Tolerance & Recovery](#fault-tolerance--recovery)
7. [Data Quality & Validation](#data-quality--validation)
8. [Real-World Scenarios](#real-world-scenarios)

---

## Streaming Fundamentals

### Q1: Explain the difference between batch processing and stream processing. Why did we implement a streaming pipeline in our platform?

**Answer:**

**Batch Processing vs. Stream Processing:**

| Aspect | Batch Processing | Stream Processing |
|--------|------------------|-------------------|
| **Data Scope** | Bounded (finite dataset) | Unbounded (continuous stream) |
| **Latency** | High (minutes to hours) | Low (seconds to sub-second) |
| **Processing Model** | Process complete dataset | Process events as they arrive |
| **Use Cases** | Historical analysis, reports | Real-time monitoring, fraud detection |
| **Complexity** | Simpler (well-defined start/end) | Complex (windowing, watermarks, late data) |
| **Resource Usage** | Periodic spikes | Continuous, stable |
| **Example** | Nightly ETL job | Real-time transaction processing |

**Detailed Comparison:**

**1. Data Model:**

```python
# Batch Processing
# Process yesterday's transactions at 2 AM
transactions_yesterday = spark.read \
    .parquet("s3://data/transactions/date=2025-11-18/*.parquet")

result = transactions_yesterday \
    .groupBy("account_id") \
    .agg(sum("amount").alias("daily_total"))

result.write.parquet("s3://output/daily_totals/2025-11-18/")

# Characteristics:
# - Fixed input size (yesterday's data)
# - Runs once per day
# - Results available after processing (30 min - 2 hours)
# - If fails, retry next night
```

```python
# Stream Processing
# Process transactions as they arrive (continuous)
stream_df = spark.readStream \
    .format("kafka") \
    .option("subscribe", "transactions") \
    .load()

result = stream_df \
    .groupBy("account_id", window("timestamp", "1 hour")) \
    .agg(sum("amount").alias("hourly_total"))

result.writeStream \
    .outputMode("update") \
    .trigger(processingTime="30 seconds") \
    .start()

# Characteristics:
# - Unbounded input (never ends)
# - Runs 24/7
# - Results available in real-time (~30s latency)
# - Must handle late/out-of-order data
```

**2. Latency Characteristics:**

```
Batch Processing Timeline:
Event → Buffered (0-24 hours) → Processed (30 min) → Available
Total Latency: 30 min - 24.5 hours

Stream Processing Timeline:
Event → Kafka (< 1ms) → Spark (30s batch) → Iceberg (< 30s) → Available
Total Latency: ~30-60 seconds
```

**3. Windowing:**

**Batch:**
```python
# Natural boundaries (daily, weekly)
daily_data = df.filter("date = '2025-11-18'")

# Tumbling windows implicit (1 day)
result = daily_data.groupBy("account_id").agg(...)
```

**Streaming:**
```python
# Explicit windowing (time-based or count-based)
# Tumbling window (non-overlapping)
result = stream_df.groupBy(
    "account_id",
    window("timestamp", "1 hour")  # 00:00-01:00, 01:00-02:00, ...
).agg(count("*"))

# Sliding window (overlapping)
result = stream_df.groupBy(
    "account_id",
    window("timestamp", "1 hour", "30 minutes")  # 00:00-01:00, 00:30-01:30, ...
).agg(count("*"))

# Session window (gap-based)
# Process transactions until 5 min of inactivity
```

---

**Why We Implemented Streaming in Our Platform:**

**1. Business Requirements:**

```
Use Case: Banking Transaction Monitoring
- Fraud detection: Flag suspicious transactions in real-time
- Account alerts: Notify customers of large withdrawals immediately
- Risk management: Track exposure in real-time
- Compliance: Audit trail with minimal latency

Latency Requirement: < 1 minute (batch processing would be 30 min - 24 hours)
```

**2. Data Freshness:**

```python
# Batch: Data freshness = processing interval
# If batch runs daily at 2 AM, data is 2-26 hours old

# Streaming: Data freshness = micro-batch interval (30 seconds)
# Data in Bronze layer is at most 60 seconds old
```

**3. Resource Efficiency:**

```
Batch:
- CPU idle 23.5 hours/day (if daily batch)
- Spikes to 100% during processing (30 min)
- Need to provision for peak load

Streaming:
- CPU steady at ~40-60% (24/7)
- No spikes, predictable resource usage
- Can provision for average load
```

**4. Operational Simplicity:**

```bash
# Batch: Complex scheduling
# - Airflow/Cron jobs
# - Dependency management (upstream/downstream jobs)
# - Late data handling (reprocess yesterday?)

# Streaming: Simple long-running job
# - Single Spark application (runs 24/7)
# - No scheduling overhead
# - Late data handled automatically (watermarks)
```

**5. Future-Proofing:**

```
Current: Streaming Bronze (30s latency)
         Batch Silver/Gold (daily)

Future: Streaming Silver (real-time fraud detection)
        Streaming Gold (real-time dashboards)

With streaming Bronze, we can evolve to full streaming without re-architecting
```

---

**Trade-offs We Accepted:**

**Disadvantages of Streaming:**

1. **Complexity:**
   - Must handle late/out-of-order data
   - Watermarks, event time vs. processing time
   - State management (checkpoints)

2. **Operational Overhead:**
   - 24/7 monitoring (batch can fail overnight, retry next day)
   - Requires robust alerting
   - More complex debugging (continuous vs. one-shot)

3. **Cost:**
   - Continuous resource usage (Spark cluster 24/7)
   - Kafka cluster for buffering
   - Higher infrastructure cost vs. batch

**Why We Accepted These Trade-offs:**

- **Business Value:** Real-time fraud detection saves $100K+/month (vs. cost of ~$5K/month infrastructure)
- **Scalability:** Streaming handles 1000x growth without re-architecture
- **User Experience:** Customers get instant alerts (vs. next-day emails)

---

**Hybrid Architecture (Best of Both Worlds):**

```
Bronze Layer (Streaming):
- Real-time ingestion from Kafka
- 30-second micro-batches
- Append-only, raw data
Purpose: Low latency, data freshness

Silver Layer (Batch):
- Daily processing (Bronze → Silver)
- Complex transformations (deduplication, validation)
- Runs at 2 AM (off-peak)
Purpose: Data quality, resource efficiency

Gold Layer (Batch):
- Daily aggregations (Silver → Gold)
- Pre-computed summaries for dashboards
- Runs at 3 AM
Purpose: Query performance
```

**Why Hybrid:**
- **Bronze Streaming:** Meets latency requirements (<1 min)
- **Silver/Gold Batch:** Avoids complexity of streaming aggregations, runs off-peak

**Future Evolution:**
```
Phase 2: Streaming Silver (if needed)
- Real-time fraud detection (seconds, not hours)
- Streaming deduplication, validation

Phase 3: Streaming Gold
- Real-time dashboards (live KPIs)
- Complex event processing (CEP)
```

---

**Benchmarking: Streaming vs. Batch for Our Use Case**

| Metric | Batch (Daily) | Streaming (30s) | Improvement |
|--------|---------------|-----------------|-------------|
| **Data Latency** | 12-36 hours | 30-60 seconds | **720x faster** |
| **Fraud Detection** | Next day | Real-time | **Prevent fraud, not just detect** |
| **Resource Utilization** | Spiky (30 min peak) | Steady (24/7) | **Better capacity planning** |
| **Operational Complexity** | Simple (Cron job) | Complex (24/7 monitoring) | **Trade-off** |
| **Infrastructure Cost** | $2K/month (part-time cluster) | $5K/month (24/7 cluster) | **2.5x higher** |
| **Business Value** | Baseline | $100K+/month (fraud prevention) | **20x ROI** |

---

**Alternative Architectures Considered:**

**1. Micro-Batch (Chosen Approach):**
```
Characteristics:
- Kafka → Spark Streaming (30s micro-batches) → Iceberg
- Latency: 30-60 seconds
- Complexity: Medium
- Throughput: 333 records/sec (10K per 30s batch)

Pros:
✅ Balance of latency and complexity
✅ Exactly-once semantics (checkpointing)
✅ Familiar Spark API
✅ Good for our 0.5 records/sec ingestion rate

Cons:
⚠️ Not true real-time (30s delay)
⚠️ Overhead from batch scheduling
```

**2. True Streaming (Apache Flink):**
```
Characteristics:
- Kafka → Flink (event-by-event) → Iceberg
- Latency: < 1 second
- Complexity: High
- Throughput: 100K+ records/sec

Pros:
✅ Sub-second latency
✅ Advanced event time processing
✅ Exactly-once semantics

Cons:
❌ Steeper learning curve (new API)
❌ More operational complexity
❌ Overkill for our 0.5 records/sec rate
❌ Not needed (30s latency is acceptable)
```

**3. Lambda Architecture (Batch + Stream):**
```
Characteristics:
- Batch layer: Spark batch (historical data)
- Speed layer: Spark Streaming (recent data)
- Serving layer: Merge results

Pros:
✅ Both batch and real-time
✅ Fault tolerance

Cons:
❌ Duplicate logic (batch + stream)
❌ Reconciliation complexity
❌ Superseded by Kappa (streaming-only)
```

**4. Kappa Architecture (Stream-Only):**
```
Characteristics:
- Single streaming pipeline (no batch)
- Reprocess by replaying Kafka logs

Pros:
✅ Single codebase
✅ No dual logic
✅ Kafka retention allows reprocessing

Cons:
❌ Kafka retention limits (7 days in our setup)
❌ Hard to reprocess months of data
❌ Not suitable for historical analysis
```

**Our Choice: Medallion + Micro-Batch Streaming**
```
- Streaming Bronze (Kafka → Spark → Iceberg)
- Batch Silver/Gold (Spark batch jobs)
- Kafka retention: 7 days (reprocessing window)
- Iceberg Bronze: Long-term storage (years)

Rationale:
✅ Meets latency requirements (< 1 min)
✅ Familiar technology (Spark)
✅ Hybrid approach (streaming + batch)
✅ Scalable to 1000x growth
✅ Future-proof (can migrate to full streaming)
```

---

## Kafka Architecture & Operations

### Q2: Describe our Kafka setup in detail. How does KRaft mode work, and how do you manage topics, partitions, and replication?

**Answer:**

**Kafka Cluster Architecture:**

**1. Cluster Configuration:**

```yaml
Kafka Cluster:
  - Mode: KRaft (Kafka Raft metadata mode, no Zookeeper)
  - Brokers: 3 (kafka1, kafka2, kafka3)
  - Quorum: All 3 brokers are controller voters
  - Network: Docker network (data-platform)
  - Storage: Docker volumes (persistent)

Broker Configuration:
  kafka1:
    - Process ID: 1
    - Ports:
        - Internal: 29092 (inter-broker, Spark)
        - External: 9092 (localhost access)
        - Controller: 9093 (KRaft consensus)
    - Roles: broker,controller
    - Data: /var/lib/kafka/data (Docker volume)
    - JMX: 10001 (Prometheus metrics)

  kafka2:
    - Process ID: 2
    - Ports: 29092, 9093, 9093
    - JMX: 10002

  kafka3:
    - Process ID: 3
    - Ports: 29092, 9094, 9093
    - JMX: 10003
```

**2. KRaft Mode (Kafka Raft Metadata Mode):**

**Traditional Kafka (with Zookeeper):**
```
Producer → Kafka Broker → Zookeeper (metadata)
                ↓
            Consumer

Metadata in Zookeeper:
- Broker registry
- Topic configuration
- Partition assignments
- Controller election
- Consumer group offsets (legacy)

Problems:
❌ Zookeeper dependency (another system to manage)
❌ Dual consensus (Kafka replication + Zookeeper quorum)
❌ Scalability bottleneck (Zookeeper metadata operations)
❌ Operational complexity (6+ nodes: 3 Kafka + 3 Zookeeper)
```

**KRaft Mode (No Zookeeper):**
```
Producer → Kafka Broker (metadata + data)
                ↓
            Consumer

Metadata in Kafka (KRaft):
- Brokers manage metadata internally
- Raft consensus for metadata replication
- Controller: One broker elected as controller
- Metadata log: Special internal topic (__cluster_metadata)

Benefits:
✅ No Zookeeper (3 fewer nodes)
✅ Single consensus protocol (Raft)
✅ Faster metadata operations (no network hop to Zookeeper)
✅ Simplified operations
✅ Better scalability (millions of partitions)
```

**KRaft Architecture:**

```
┌────────────────────────────────────────────────────┐
│              Kafka Cluster (KRaft Mode)             │
├────────────────────────────────────────────────────┤
│                                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐│
│  │   Broker 1  │  │   Broker 2  │  │   Broker 3  ││
│  │             │  │             │  │             ││
│  │ Controller? │  │ Controller? │  │ Controller? ││
│  │ ✅ ACTIVE   │  │ ⏸ STANDBY   │  │ ⏸ STANDBY   ││
│  │             │  │             │  │             ││
│  │ Metadata    │  │ Metadata    │  │ Metadata    ││
│  │ Log (Raft)  │  │ Log (Raft)  │  │ Log (Raft)  ││
│  │             │  │             │  │             ││
│  │ Data        │  │ Data        │  │ Data        ││
│  │ Partitions  │  │ Partitions  │  │ Partitions  ││
│  └─────────────┘  └─────────────┘  └─────────────┘│
│         ↑                ↑                ↑        │
│         └────────────────┴────────────────┘        │
│              Raft Consensus (Quorum)               │
└────────────────────────────────────────────────────┘
```

**KRaft Configuration:**

```properties
# server.properties (same for all brokers, except node.id)

# Process roles (combined broker + controller)
process.roles=broker,controller
node.id=1  # Unique: 1, 2, 3

# Controller quorum (all 3 brokers)
controller.quorum.voters=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093

# Listeners
listeners=PLAINTEXT://:29092,EXTERNAL://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://kafka1:29092,EXTERNAL://localhost:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

# Inter-broker communication
inter.broker.listener.name=PLAINTEXT

# Controller communication
controller.listener.names=CONTROLLER

# Metadata log directory
metadata.log.dir=/var/lib/kafka/data/__cluster_metadata
```

**Cluster Initialization:**

```bash
# 1. Generate cluster ID (once per cluster)
CLUSTER_ID=$(docker exec kafka1 kafka-storage random-uuid)
echo "Cluster ID: $CLUSTER_ID"

# 2. Format storage on each broker (first time only)
docker exec kafka1 kafka-storage format \
  -t $CLUSTER_ID \
  -c /etc/kafka/server.properties

docker exec kafka2 kafka-storage format \
  -t $CLUSTER_ID \
  -c /etc/kafka/server.properties

docker exec kafka3 kafka-storage format \
  -t $CLUSTER_ID \
  -c /etc/kafka/server.properties

# 3. Start brokers (docker-compose up)
# Brokers form quorum, elect controller
```

**Controller Election (Raft):**

```
1. Brokers start, form quorum (need 2/3 = 2 brokers)
2. Vote for controller (Raft leader election)
3. Broker 1 becomes controller (lowest ID typically wins)
4. Controller manages:
   - Partition assignments
   - Leader election (for data partitions)
   - Topic creation/deletion
   - Replication changes
5. Standby controllers replicate metadata log
6. If controller fails:
   - Standby promoted (within seconds)
   - No downtime for clients
```

**Metadata Log (__cluster_metadata):**

```
/var/lib/kafka/data/__cluster_metadata-0/
├─ 00000000000000000000.log (metadata events)
├─ 00000000000000000000.index
├─ 00000000000000000000.timeindex
└─ leader-epoch-checkpoint

# Metadata events (Raft log):
- Topic creation/deletion
- Partition assignment
- Replica changes
- Configuration changes
- Controller election

# Example metadata event:
{
  "type": "TopicRecord",
  "name": "banking.transactions.raw",
  "partitions": [
    {"partition": 0, "leader": 1, "replicas": [1, 2]},
    {"partition": 1, "leader": 2, "replicas": [2, 3]},
    {"partition": 2, "leader": 3, "replicas": [3, 1]}
  ]
}
```

---

**3. Topic Management:**

**Our Topics:**

```bash
# List topics
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --list

# Output:
banking.transactions.raw      # Main transactional data
banking.accounts.cdc          # Account change data capture
banking.customers.enriched    # Enriched customer data
banking.dlq                   # Dead letter queue
```

**Topic Configuration:**

**1. banking.transactions.raw (Primary Topic):**

```bash
# Create topic
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --create \
  --topic banking.transactions.raw \
  --partitions 3 \
  --replication-factor 2 \
  --config retention.ms=604800000 \  # 7 days
  --config compression.type=snappy \
  --config min.insync.replicas=1 \
  --config cleanup.policy=delete

# Describe topic
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --describe --topic banking.transactions.raw

# Output:
Topic: banking.transactions.raw
  PartitionCount: 3
  ReplicationFactor: 2
  Configs: retention.ms=604800000, compression.type=snappy, min.insync.replicas=1

Partition: 0
  Leader: 1
  Replicas: 1,2
  Isr: 1,2  # In-Sync Replicas

Partition: 1
  Leader: 2
  Replicas: 2,3
  Isr: 2,3

Partition: 2
  Leader: 3
  Replicas: 3,1
  Isr: 3,1
```

**Partition Strategy:**

```
Why 3 Partitions?

1. Parallelism:
   - Spark creates 1 task per partition
   - 3 partitions = 3 parallel readers (maps to 2 workers × 2 cores)

2. Scalability:
   - Can add more consumers (up to 3) for horizontal scaling
   - Each consumer reads 1 partition

3. Ordering:
   - No key specified (round-robin distribution)
   - Ordering within partition (not across partitions)

4. Failure Isolation:
   - If 1 partition fails (leader election), others continue

Partitioning Logic:
partition = hash(key) % num_partitions  # If key specified
partition = round_robin               # If no key (our case)

# Our data generator doesn't specify key:
producer.send('banking.transactions.raw', value=transaction)
# Result: Messages distributed round-robin across 3 partitions
```

**Replication Strategy:**

```
Replication Factor: 2
- Each partition has 1 leader + 1 follower
- Leader handles all reads/writes
- Follower replicates data (async or sync)

Min In-Sync Replicas: 1
- Producer acks=all requires min.insync.replicas to acknowledge
- With min.insync=1, only leader must ACK
- With min.insync=2, leader + 1 follower must ACK

Trade-offs:
┌───────────────┬──────────────┬────────────┬─────────────┐
│ Configuration │ Availability │ Durability │ Latency     │
├───────────────┼──────────────┼────────────┼─────────────┤
│ RF=2, min=1   │ High         │ Good       │ Low         │
│ (our setup)   │ (lose 1 broker) │ (1 copy)  │ (leader only) │
├───────────────┼──────────────┼────────────┼─────────────┤
│ RF=3, min=2   │ Very High    │ Excellent  │ Medium      │
│ (production)  │ (lose 2)     │ (2 copies) │ (wait for 2) │
├───────────────┼──────────────┼────────────┼─────────────┤
│ RF=1, min=1   │ Low          │ None       │ Very Low    │
│ (dev only)    │ (lose 0)     │ (no backup)│ (no wait)   │
└───────────────┴──────────────┴────────────┴─────────────┘

Partition Distribution Example:
Partition 0: Leader=Broker1, Follower=Broker2
Partition 1: Leader=Broker2, Follower=Broker3
Partition 2: Leader=Broker3, Follower=Broker1

# Even distribution across brokers:
Broker 1: Leader for P0, Follower for P2
Broker 2: Leader for P1, Follower for P0
Broker 3: Leader for P2, Follower for P1
```

**Retention Policy:**

```properties
# banking.transactions.raw
retention.ms=604800000  # 7 days (168 hours)
cleanup.policy=delete   # Delete old segments (not compaction)

# Why 7 days?
1. Reprocessing window:
   - Spark checkpoint failures → Replay from Kafka
   - Max 7 days of backfill

2. Storage cost:
   - 0.5 records/sec × 86,400 sec/day × 7 days = ~302K records
   - ~300 KB/day × 7 = ~2 MB (compressed)
   - Minimal storage cost

3. Bronze layer is source of truth:
   - After 7 days, data in Iceberg Bronze
   - No need for Kafka retention beyond reprocessing window
```

**2. banking.accounts.cdc (Change Data Capture):**

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --create \
  --topic banking.accounts.cdc \
  --partitions 1 \
  --replication-factor 2 \
  --config cleanup.policy=compact \  # Log compaction (not delete)
  --config min.cleanable.dirty.ratio=0.5 \
  --config segment.ms=3600000  # 1 hour

# Compaction:
# - Keeps only latest value for each key
# - Deletes old updates (key=account_id, value=account state)
# - Use case: Maintain current state of all accounts
```

**3. banking.dlq (Dead Letter Queue):**

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --create \
  --topic banking.dlq \
  --partitions 1 \
  --replication-factor 2 \
  --config retention.ms=2592000000  # 30 days
  --config cleanup.policy=delete

# Purpose: Failed messages (parsing errors, validation failures)
# Retention: 30 days (for debugging)
```

---

**4. Partition Management:**

**Increasing Partitions:**

```bash
# Current: 3 partitions
# Target: 30 partitions (for higher throughput)

# Increase partitions
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --alter \
  --topic banking.transactions.raw \
  --partitions 30

# ⚠️ WARNING: Cannot decrease partitions (only increase)
# ⚠️ Existing data stays in old partitions (not redistributed)

# Result:
# - Partition 0-2: Old data (pre-change)
# - Partition 3-29: New data (post-change)
# - Consumers must handle all 30 partitions
```

**Partition Reassignment (Rebalancing):**

```bash
# Use case: Add new broker, rebalance partitions

# 1. Generate reassignment plan
docker exec kafka1 kafka-reassign-partitions --bootstrap-server kafka1:29092 \
  --topics-to-move-json-file topics.json \
  --broker-list "1,2,3,4" \  # Include new broker 4
  --generate

# 2. Review plan (reassignment.json)
# Example:
# Partition 0: [1, 2] → [1, 4]  # Move follower to broker 4
# Partition 1: [2, 3] → [2, 4]
# Partition 2: [3, 1] → [3, 4]

# 3. Execute reassignment
docker exec kafka1 kafka-reassign-partitions --bootstrap-server kafka1:29092 \
  --reassignment-json-file reassignment.json \
  --execute

# 4. Verify reassignment
docker exec kafka1 kafka-reassign-partitions --bootstrap-server kafka1:29092 \
  --reassignment-json-file reassignment.json \
  --verify
```

**Preferred Leader Election:**

```bash
# Force leadership back to preferred replica (for load balancing)
docker exec kafka1 kafka-leader-election --bootstrap-server kafka1:29092 \
  --election-type preferred \
  --all-topic-partitions

# Use case: After broker restart, rebalance leaders
```

---

**5. Monitoring & Operations:**

**Key Metrics:**

```bash
# 1. Under-replicated partitions (should be 0)
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --describe --under-replicated-partitions

# 2. Offline partitions (should be 0)
# Check via JMX: kafka.controller:type=KafkaController,name=OfflinePartitionsCount

# 3. Topic size
docker exec kafka1 kafka-log-dirs --bootstrap-server kafka1:29092 \
  --describe --topic-list banking.transactions.raw

# Output:
# Partition 0: 1.2 MB (5,000 messages)
# Partition 1: 1.1 MB (4,800 messages)
# Partition 2: 1.3 MB (5,200 messages)
```

**Consumer Lag (Not Applicable for Our Setup):**

```bash
# Standard consumer groups:
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 \
  --describe --group spark-streaming-group

# ⚠️ Our Spark Streaming job does NOT use consumer groups
# Uses checkpoint-based offset management
# No lag metrics in Kafka (check Spark UI instead)
```

**Log Segments:**

```bash
# Each partition has multiple log segments
/var/lib/kafka/data/banking.transactions.raw-0/
├─ 00000000000000000000.log (segment 1, offsets 0-9999)
├─ 00000000000000010000.log (segment 2, offsets 10000-19999)
└─ 00000000000000020000.log (segment 3, offsets 20000-...)

# Segment size (default: 1 GB or 7 days)
segment.bytes=1073741824  # 1 GB
segment.ms=604800000      # 7 days

# Retention: Delete entire segments (not individual messages)
```

---

## Spark Structured Streaming

### Q3: Walk through our Spark Structured Streaming job (kafka_to_iceberg_streaming.py). Explain the foreachBatch pattern and why we use it.

**Answer:**

**Streaming Job Architecture:**

```python
# File: spark/jobs/kafka_to_iceberg_streaming.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1. Initialize Spark with Iceberg support
spark = SparkSession.builder \
    .appName("KafkaToIcebergStreaming") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. Define schema for JSON data
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("merchant", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("device_info", MapType(StringType(), StringType()), True),
    StructField("location", StructType([
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ]), True),
    StructField("is_fraud", BooleanType(), True),
    StructField("status", StringType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

# 3. Read from Kafka (streaming source)
KAFKA_BOOTSTRAP_SERVERS = "kafka1:29092,kafka2:29092,kafka3:29092"
KAFKA_TOPIC = "banking.transactions.raw"
CHECKPOINT_LOCATION = "s3a://lakehouse/checkpoints/bronze_transactions/"

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \  # First run: process all data
    .option("maxOffsetsPerTrigger", "10000") \  # Limit per batch
    .load()

# 4. Parse JSON from Kafka value field
parsed_df = kafka_df.select(
    col("key").cast("string").alias("key"),
    from_json(col("value").cast("string"), schema).alias("data"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

# 5. Add processing metadata
enriched_df = parsed_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("date_partition", to_date(col("timestamp")))

# 6. Write to Iceberg using foreachBatch
def write_to_iceberg_bronze(batch_df, batch_id):
    """
    Write micro-batch to Iceberg Bronze table
    Called for each micro-batch (every 30 seconds)
    """
    if batch_df.count() == 0:
        print(f"Batch {batch_id}: No data to process")
        return

    print(f"Batch {batch_id}: Processing {batch_df.count()} records")

    # Write to Iceberg (ACID commit)
    batch_df.writeTo("iceberg.bronze.transactions") \
        .option("write-format", "parquet") \
        .option("compression-codec", "gzip") \
        .append()

    print(f"Batch {batch_id}: Successfully written to Bronze")

# 7. Start streaming query
query = enriched_df \
    .writeStream \
    .foreachBatch(write_to_iceberg_bronze) \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

# 8. Wait for termination (runs forever)
query.awaitTermination()
```

---

**Why foreachBatch Pattern?**

**Alternative 1: Direct Write (Not Supported for Iceberg Streaming):**

```python
# ❌ This doesn't work for Iceberg
query = enriched_df \
    .writeStream \
    .format("iceberg") \  # ❌ Not supported in streaming mode
    .option("path", "s3a://lakehouse/bronze/transactions") \
    .start()

# Error: "iceberg" source does not support streaming writes
```

**Alternative 2: Parquet Sink (No ACID, No Table Management):**

```python
# ⚠️ Works, but no ACID guarantees
query = enriched_df \
    .writeStream \
    .format("parquet") \
    .option("path", "s3a://lakehouse/bronze/transactions") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

# Problems:
# ❌ No Iceberg table metadata (snapshots, manifests)
# ❌ No schema evolution
# ❌ No time travel
# ❌ No ACID guarantees (files visible before complete write)
# ❌ No compaction (small file problem)
# ❌ Cannot query with Trino (no Hive Metastore registration)
```

**Alternative 3: foreachBatch (Chosen Approach):**

```python
# ✅ Full control over batch processing
query = enriched_df \
    .writeStream \
    .foreachBatch(write_to_iceberg_bronze) \  # Custom function
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

# Benefits:
# ✅ Use batch DataFrame API (full feature set)
# ✅ Iceberg ACID commits (atomic snapshots)
# ✅ Schema evolution support
# ✅ Time travel support
# ✅ Trino can query immediately after batch
# ✅ Custom logic per batch (validation, error handling)
```

---

**foreachBatch Execution Flow:**

```
┌─────────────────────────────────────────────────────┐
│         Spark Structured Streaming Engine           │
└─────────────────────────────────────────────────────┘
                      │
            ┌─────────┴─────────┐
            │ Trigger Interval  │
            │   (30 seconds)    │
            └─────────┬─────────┘
                      │
            ┌─────────▼─────────┐
            │ Read from Kafka   │
            │ (max 10K offsets) │
            └─────────┬─────────┘
                      │
            ┌─────────▼─────────┐
            │ Create Batch DF   │
            │ (micro-batch)     │
            └─────────┬─────────┘
                      │
            ┌─────────▼─────────────────────────┐
            │ foreachBatch Function             │
            │ write_to_iceberg_bronze(batch_df, │
            │                         batch_id) │
            └─────────┬─────────────────────────┘
                      │
            ┌─────────▼─────────┐
            │ Batch Processing  │
            │ (user code)       │
            │                   │
            │ - Count records   │
            │ - Validate data   │
            │ - Transform       │
            │ - Write to Iceberg│
            └─────────┬─────────┘
                      │
            ┌─────────▼─────────┐
            │ Iceberg Commit    │
            │ (atomic)          │
            └─────────┬─────────┘
                      │
            ┌─────────▼─────────┐
            │ Update Checkpoint │
            │ (commit offsets)  │
            └─────────┬─────────┘
                      │
            ┌─────────▼─────────┐
            │ Wait for Next     │
            │ Trigger (30s)     │
            └───────────────────┘
```

**Key Characteristics:**

1. **Batch DataFrame (Not Streaming DataFrame):**
   ```python
   def write_to_iceberg_bronze(batch_df, batch_id):
       # batch_df is a regular DataFrame (not streaming)
       # Can use ANY DataFrame operation:

       # Count (triggers action)
       count = batch_df.count()

       # Filter, transform
       filtered_df = batch_df.filter(col("amount") > 0)

       # Join with static data
       joined_df = batch_df.join(accounts_df, "account_id")

       # Aggregate
       summary_df = batch_df.groupBy("merchant").agg(sum("amount"))

       # Write to multiple sinks
       batch_df.writeTo("iceberg.bronze.transactions").append()
       batch_df.write.parquet("s3://backup/...")
   ```

2. **Batch ID (Unique Identifier):**
   ```python
   # batch_id starts at 0, increments for each micro-batch
   # Use for:
   # - Logging/debugging
   # - Idempotency (deduplicate by batch_id)
   # - Monitoring (track progress)

   def write_to_iceberg_bronze(batch_df, batch_id):
       logger.info(f"Processing batch {batch_id}")

       # Idempotent writes (check if batch already processed)
       if is_batch_processed(batch_id):
           logger.info(f"Batch {batch_id} already processed, skipping")
           return

       batch_df.writeTo("iceberg.bronze.transactions").append()
       mark_batch_processed(batch_id)
   ```

3. **Error Handling:**
   ```python
   def write_to_iceberg_bronze(batch_df, batch_id):
       try:
           # Validate data
           if batch_df.count() == 0:
               return

           # Write to Iceberg
           batch_df.writeTo("iceberg.bronze.transactions").append()

       except Exception as e:
           # Log error
           logger.error(f"Batch {batch_id} failed: {e}")

           # Write to DLQ (dead letter queue)
           batch_df.write \
               .format("kafka") \
               .option("topic", "banking.dlq") \
               .save()

           # Re-raise to fail batch (Spark will retry)
           raise e
   ```

4. **Multiple Outputs:**
   ```python
   def write_to_iceberg_bronze(batch_df, batch_id):
       # Write to multiple sinks in single batch
       # (all-or-nothing if within transaction)

       # 1. Write to Bronze Iceberg table
       batch_df.writeTo("iceberg.bronze.transactions").append()

       # 2. Write to backup (Parquet)
       batch_df.write.mode("append") \
           .parquet(f"s3://backups/bronze/{batch_id}/")

       # 3. Write metrics to monitoring
       metrics_df = batch_df.groupBy().agg(
           count("*").alias("record_count"),
           sum("amount").alias("total_amount")
       )
       metrics_df.write.format("prometheus").save()  # Hypothetical

       # 4. Write fraud alerts to Kafka
       fraud_df = batch_df.filter(col("is_fraud") == True)
       fraud_df.write \
           .format("kafka") \
           .option("topic", "banking.fraud_alerts") \
           .save()
   ```

---

**Checkpoint Management:**

**Checkpoint Directory Structure:**

```
s3a://lakehouse/checkpoints/bronze_transactions/
├─ offsets/
│   ├─ 0   # Batch 0: {"banking.transactions.raw":{"0":0,"1":0,"2":0}}
│   ├─ 1   # Batch 1: {"banking.transactions.raw":{"0":500,"1":500,"2":500}}
│   ├─ 2   # Batch 2: {"banking.transactions.raw":{"0":1000,"1":1000,"2":1000}}
│   └─ 3   # ...
├─ commits/
│   ├─ 0   # Batch 0 committed successfully
│   ├─ 1   # Batch 1 committed
│   └─ 2   # ...
├─ metadata
└─ sources/
    └─ 0/  # Source metadata (Kafka topic, partitions)
```

**Checkpoint Write-Ahead Log (WAL) Protocol:**

```
For each micro-batch:

1. Spark reads offset range from Kafka
   Example: Partition 0: 0-500, Partition 1: 0-500, Partition 2: 0-500

2. Writes offset metadata to checkpoint/offsets/<batch_id>
   Content: {"banking.transactions.raw":{"0":500,"1":500,"2":500}}

3. Calls foreachBatch(batch_df, batch_id)
   - User code executes
   - Iceberg commit (atomic)

4. If foreachBatch succeeds:
   - Write checkpoint/commits/<batch_id> (empty file, existence = success)
   - Kafka offsets now committed

5. If foreachBatch fails:
   - No commit file written
   - Next run: Re-read checkpoint/offsets/<batch_id>, reprocess same data

6. Next batch: batch_id + 1
```

**Exactly-Once Guarantees:**

```
Scenario 1: Normal Processing
1. Read offsets 0-500 (batch 0)
2. Write checkpoint/offsets/0
3. foreachBatch: Write to Iceberg (success)
4. Write checkpoint/commits/0
5. Next batch (batch 1, offsets 500-1000)
Result: ✅ Exactly once

Scenario 2: Failure in foreachBatch
1. Read offsets 0-500 (batch 0)
2. Write checkpoint/offsets/0
3. foreachBatch: Write to Iceberg (fails - network error)
4. No commit file (checkpoint/commits/0 not written)
5. Spark restarts
6. Reads checkpoint/offsets/0 (last uncommitted batch)
7. Reprocesses offsets 0-500
Result: ✅ Exactly once (idempotent retry)

Scenario 3: Failure After Iceberg, Before Checkpoint
1. Read offsets 0-500 (batch 0)
2. Write checkpoint/offsets/0
3. foreachBatch: Write to Iceberg (success, snapshot created)
4. Spark crashes BEFORE writing checkpoint/commits/0
5. Spark restarts
6. Reads checkpoint/offsets/0 (no commit file)
7. Reprocesses offsets 0-500
8. Writes duplicate data to Iceberg
Result: ⚠️ At-least-once (duplicate snapshot)

Mitigation: Deduplication in Silver layer
```

---

**Trigger Interval Tuning:**

```python
# Current: 30 seconds
.trigger(processingTime="30 seconds")

# Trade-offs:
┌──────────────┬────────────┬─────────────┬───────────────┐
│ Interval     │ Latency    │ Throughput  │ Overhead      │
├──────────────┼────────────┼─────────────┼───────────────┤
│ 1 second     │ Very Low   │ Low         │ High (sched)  │
│ 10 seconds   │ Low        │ Medium      │ Medium        │
│ 30 seconds   │ Medium     │ High        │ Low (optimal) │
│ 60 seconds   │ High       │ Very High   │ Very Low      │
│ 5 minutes    │ Very High  │ Batch-like  │ Minimal       │
└──────────────┴────────────┴─────────────┴───────────────┘

# Recommendation: 30 seconds for our use case
# - Balances latency (< 1 min) and overhead
# - Iceberg commit overhead (~1s) amortized over 30s batch
# - Enough time for checkpoint writes
```

**maxOffsetsPerTrigger Tuning:**

```python
# Current: 10,000 records per batch
.option("maxOffsetsPerTrigger", "10000")

# Purpose: Limit batch size (prevent OOM)
# With 30s trigger + 10K limit:
#   Max throughput = 10,000 / 30s = 333 records/sec

# If ingestion rate > 333 rec/sec:
#   - Lag builds up in Kafka
#   - Increase maxOffsetsPerTrigger or decrease trigger interval

# If ingestion rate < 333 rec/sec (our case: 0.5 rec/sec):
#   - Batches are smaller (e.g., 15 records per 30s batch)
#   - Could increase trigger interval to 5 min (reduce overhead)
```

---

