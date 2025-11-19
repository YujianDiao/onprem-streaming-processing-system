# Data Platform Engineer - Interview Questions & Answers

**Role:** Data Platform Engineer
**Platform:** On-Premise Streaming Data Lakehouse
**Date:** 2025-11-19

---

## Table of Contents

1. [Architecture & Design](#architecture--design)
2. [Data Storage & Formats](#data-storage--formats)
3. [Query Engines & Performance](#query-engines--performance)
4. [Metadata Management](#metadata-management)
5. [Platform Scalability](#platform-scalability)
6. [Data Governance & Security](#data-governance--security)
7. [Integration & APIs](#integration--apis)
8. [Platform Evolution](#platform-evolution)

---

## Architecture & Design

### Q1: Explain the medallion architecture (Bronze/Silver/Gold) implemented in our platform. Why did we choose this pattern, and what are the trade-offs?

**Answer:**

**Medallion Architecture Overview:**

The medallion architecture is a data lakehouse design pattern that organizes data into three progressive layers, each with increasing levels of quality and refinement:

```
Bronze Layer (Raw Zone)
    ↓
Silver Layer (Curated Zone)
    ↓
Gold Layer (Business Zone)
```

**Implementation in Our Platform:**

**1. Bronze Layer: Raw Ingestion Zone**

```sql
Table: iceberg.bronze.transactions
Source: Kafka topic (banking.transactions.raw)
Schema: All raw JSON fields (14 columns)
Partition: BY date_partition (YYYY-MM-DD)
Format: Parquet + GZIP compression
Purpose: Immutable, append-only raw data
```

**Characteristics:**
- **Data Quality:** No validation, as-is from source
- **Schema:** Dynamic, accepts any fields from JSON
- **Processing:** Simple 1:1 copy from Kafka to Iceberg
- **Use Case:** Data lineage, auditing, reprocessing
- **Retention:** Long-term (years), rarely deleted
- **Update Pattern:** Append-only, no updates/deletes

**Implementation:**
```python
# Spark Streaming: Kafka → Bronze
kafka_df.select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("date_partition", to_date(col("timestamp"))) \
    .writeTo("iceberg.bronze.transactions") \
    .append()
```

**2. Silver Layer: Curated/Cleaned Zone**

```sql
Table: iceberg.silver.transactions
Source: iceberg.bronze.transactions (batch transformation)
Schema: Validated, typed, deduplicated
Partition: BY days(date_partition) -- Hidden partitioning
Format: Parquet + GZIP
Purpose: Analytics-ready, high-quality data
```

**Characteristics:**
- **Data Quality:** Validated, deduplicated, enriched
- **Schema:** Strictly typed (DECIMAL, TIMESTAMP, etc.)
- **Processing:** Data quality rules, business logic
- **Use Case:** Data science, ML feature engineering
- **Retention:** Medium-term (months to years)
- **Update Pattern:** Merge/upsert for CDC, mostly append

**Transformations Applied:**
```python
# Batch Job: Bronze → Silver
silver_df = bronze_df \
    .dropDuplicates(["transaction_id"]) \  # Deduplication
    .filter(col("amount") > 0) \            # Validation
    .filter(col("status").isin([           # Enumeration validation
        "COMPLETED", "PENDING", "FAILED"
    ])) \
    .withColumn("amount",
        col("amount").cast("decimal(18,2)")) \  # Type enforcement
    .withColumn("is_high_value",
        col("amount") > 10000) \                 # Enrichment
    .withColumn("processing_timestamp",
        current_timestamp())
```

**Data Quality Rules:**
- Amount > 0 (reject invalid transactions)
- Status in predefined set (enum validation)
- Deduplication by `transaction_id` (idempotency)
- Fraud flagging (business logic enrichment)
- Type casting (schema enforcement)

**3. Gold Layer: Business/Aggregated Zone**

```sql
Tables:
  - iceberg.gold.daily_account_summary
  - iceberg.gold.merchant_summary

Source: iceberg.silver.transactions
Schema: Aggregated metrics, denormalized
Partition: BY days(transaction_date)
Format: Parquet + SNAPPY (faster reads)
Purpose: BI dashboards, reporting, KPIs
```

**Characteristics:**
- **Data Quality:** Highest, business-validated
- **Schema:** Denormalized, star schema style
- **Processing:** Aggregations, joins, rollups
- **Use Case:** Dashboards, executive reports
- **Retention:** Short-to-medium term (weeks to months)
- **Update Pattern:** Overwrite partitions or incremental append

**Aggregations:**
```python
# Daily Account Summary
daily_summary_df = silver_df \
    .groupBy("account_id", "date_partition") \
    .agg(
        count("*").alias("transaction_count"),
        sum(when(col("amount") < 0, col("amount")).otherwise(0)).alias("total_debits"),
        sum(when(col("amount") > 0, col("amount")).otherwise(0)).alias("total_credits"),
        sum("amount").alias("net_amount"),
        avg("amount").alias("avg_transaction_amount"),
        max("amount").alias("max_transaction_amount"),
        min("amount").alias("min_transaction_amount"),
        countDistinct("merchant").alias("unique_merchants"),
        sum(when(col("is_fraud"), 1).otherwise(0)).alias("flagged_transactions")
    )
```

**Query Performance:**
- Bronze: Full table scan (~1s for 22K rows)
- Silver: Optimized partitioning (<500ms)
- Gold: Pre-aggregated (<100ms for summaries)

---

**Why Medallion Architecture?**

**Advantages:**

1. **Data Quality Progression:**
   - Clear separation of concerns (raw vs. curated vs. business)
   - Incremental validation (fail early in Bronze, refine in Silver)
   - Easy to trace data quality issues back to source

2. **Reprocessability:**
   - Bronze layer is immutable → can always reprocess
   - Fix bugs in Silver/Gold transformations without re-ingesting
   - Time travel in Iceberg allows comparing old vs. new logic

3. **Performance Optimization:**
   - Bronze: Optimized for write throughput (simple append)
   - Silver: Optimized for read/write balance (partitioning, compaction)
   - Gold: Optimized for read performance (pre-aggregated, denormalized)

4. **Use Case Segmentation:**
   - Bronze: Data engineers, debugging, auditing
   - Silver: Data scientists, ML pipelines, exploratory analysis
   - Gold: Business analysts, dashboards, reporting

5. **Cost Optimization:**
   - Bronze: Cheap storage (compressed, infrequently accessed)
   - Silver: Hot storage (frequently queried)
   - Gold: Minimal storage (aggregated, small size)

6. **Governance & Compliance:**
   - Bronze: Raw data for regulatory compliance (GDPR, audit trails)
   - Silver: PII masking, anonymization applied here
   - Gold: Fully compliant, ready for business use

**Disadvantages/Trade-offs:**

1. **Storage Overhead:**
   - Data duplicated across 3 layers (Bronze + Silver + Gold)
   - Mitigation: Use compression, set retention policies
   - Example: 22K records = 2.2 MB (Bronze) + 1.5 MB (Silver) + 100 KB (Gold) = ~4 MB total

2. **Processing Complexity:**
   - Need to manage 3 sets of jobs (ingestion, curation, aggregation)
   - Orchestration overhead (batch jobs for Silver/Gold)
   - Mitigation: Use Airflow/Dagster for orchestration (not yet implemented)

3. **Latency:**
   - End-to-end latency: Kafka → Bronze (30s) → Silver (batch, hours) → Gold (batch, hours)
   - Real-time dashboards must query Bronze (slower) or implement streaming Silver
   - Mitigation: Streaming Silver layer for low-latency use cases

4. **Schema Management:**
   - Schema evolution must be coordinated across 3 layers
   - Bronze schema change → Update Silver transformation → Update Gold aggregation
   - Mitigation: Schema registry, automated migration scripts

5. **Debugging Complexity:**
   - Data issues may span multiple layers (Bronze OK, Silver bug)
   - Requires tracing through entire pipeline
   - Mitigation: Comprehensive logging, data quality metrics per layer

**Alternative Architectures Considered:**

**1. Single Raw Layer (Data Lake):**
```
Pros: Simple, no duplication
Cons: No data quality guarantees, slow queries, no optimization
Verdict: Not suitable for production analytics
```

**2. Two-Tier (Raw + Curated):**
```
Pros: Simpler than medallion, less duplication
Cons: Mixed use cases in curated layer (exploratory + BI), harder to optimize
Verdict: Good for small teams, but lacks segmentation
```

**3. Lambda Architecture (Batch + Speed Layer):**
```
Pros: Supports low-latency and batch workloads
Cons: Duplicate processing logic, complex reconciliation
Verdict: Superseded by medallion + streaming (Kappa-like)
```

**Our Choice: Medallion + Streaming**

We implemented a **hybrid approach**:
- **Streaming Bronze:** Real-time ingestion from Kafka (30s latency)
- **Batch Silver/Gold:** Scheduled transformations (daily or hourly)
- **Future:** Streaming Silver for low-latency analytics

**Best Practices Implemented:**

1. **Immutable Bronze:** Never update/delete, only append
2. **Idempotent Transformations:** Silver job can re-run without duplicates
3. **Partition Pruning:** All layers partitioned by date for fast queries
4. **Compression:** GZIP for Bronze/Silver (storage), SNAPPY for Gold (speed)
5. **Schema Versioning:** Iceberg tracks schema evolution across layers

**When to Use Medallion:**
- ✅ Large-scale data platforms (> 1 TB/day)
- ✅ Multiple use cases (BI, ML, exploratory)
- ✅ Strong data quality requirements
- ✅ Need for auditability and reprocessing

**When to Avoid:**
- ❌ Small datasets (< 100 GB)
- ❌ Single use case (e.g., only dashboards)
- ❌ Extreme low latency requirements (use Kappa architecture)

---

### Q2: Why did we choose Apache Iceberg over alternatives like Delta Lake or Apache Hudi? What are the key differences?

**Answer:**

**Comparison: Iceberg vs. Delta Lake vs. Hudi**

| Feature | Apache Iceberg | Delta Lake | Apache Hudi |
|---------|----------------|------------|-------------|
| **Creator** | Netflix → Apache | Databricks → Linux Foundation | Uber → Apache |
| **Open Source** | ✅ Fully open (Apache 2.0) | ⚠️ Open, but optimized for Databricks | ✅ Fully open (Apache 2.0) |
| **Engine Support** | Spark, Trino, Flink, Presto, Hive | Spark (primary), limited others | Spark (primary), Flink, Presto |
| **ACID Transactions** | ✅ Full ACID | ✅ Full ACID | ✅ Full ACID |
| **Time Travel** | ✅ Snapshot-based | ✅ Version-based | ✅ Snapshot-based |
| **Schema Evolution** | ✅ Full support (add, drop, rename) | ✅ Full support | ⚠️ Limited (add only) |
| **Partition Evolution** | ✅ Hidden partitioning (change spec) | ❌ Cannot change partitions | ⚠️ Limited |
| **File Format** | Parquet, ORC, Avro | Parquet (primary) | Parquet, ORC |
| **Metadata Storage** | Separate (Hive Metastore, Nessie, Glue) | Delta Log (JSON files) | Timeline (Avro) |
| **Cloud Agnostic** | ✅ Yes (S3, GCS, ADLS, MinIO) | ⚠️ Best on Databricks | ✅ Yes |
| **Query Performance** | ✅ Excellent (predicate pushdown, metadata pruning) | ✅ Excellent | ⚠️ Good (merge-on-read overhead) |
| **Write Performance** | ✅ Fast (copy-on-write) | ✅ Fast | ⚠️ Slower (merge-on-read, indexing) |
| **Small File Problem** | ✅ Compaction available | ✅ Auto-compaction (OPTIMIZE) | ✅ Built-in clustering |
| **Concurrency** | ✅ Optimistic concurrency | ✅ Optimistic concurrency | ⚠️ Pessimistic locking (timeline service) |
| **Streaming Support** | ✅ Spark Structured Streaming | ✅ Spark Structured Streaming | ✅ DeltaStreamer (built-in) |
| **CDC Support** | ⚠️ Manual implementation | ✅ Change Data Feed | ✅ Built-in (incremental pulls) |
| **Maturity** | ✅ Production-ready (v1.0: 2021) | ✅ Production-ready (v1.0: 2020) | ⚠️ Complex, learning curve |

---

**Why We Chose Iceberg:**

**1. True Vendor Neutrality**

```yaml
Iceberg:
  - Designed for multi-engine access (Spark, Trino, Flink, Presto)
  - No vendor lock-in (works same on AWS, GCP, Azure, on-prem)
  - Open governance (Apache Foundation)

Delta Lake:
  - Optimized for Databricks ecosystem
  - Open source, but advanced features require Databricks (Delta Sharing, CDF)
  - Limited support in Trino/Presto (requires Delta connector, not native)

Hudi:
  - Primarily Spark-focused, Presto support improving
  - Uber-centric design patterns
```

**In our case:** We use **Trino** as the primary query engine (not Spark SQL). Iceberg has **native Trino support** (`iceberg` connector), whereas Delta Lake requires a separate connector with limited features.

**2. Superior Schema Evolution**

```sql
-- Iceberg: Full schema evolution
ALTER TABLE iceberg.bronze.transactions
  ADD COLUMN new_field STRING;

ALTER TABLE iceberg.bronze.transactions
  DROP COLUMN old_field;

ALTER TABLE iceberg.bronze.transactions
  RENAME COLUMN merchant TO merchant_name;

-- All operations are metadata-only (no data rewrite)
-- Old data files remain readable with schema evolution
```

```sql
-- Delta Lake: Schema evolution supported but...
ALTER TABLE delta.bronze.transactions
  ADD COLUMN new_field STRING;  -- ✅ Supported

ALTER TABLE delta.bronze.transactions
  DROP COLUMN old_field;  -- ⚠️ Requires rewriting data (expensive)

ALTER TABLE delta.bronze.transactions
  RENAME COLUMN merchant TO merchant_name;  -- ❌ Not supported (workaround: add new + drop old)
```

**Hudi:** Only supports adding columns, not dropping/renaming.

**3. Hidden Partitioning (Critical for Our Use Case)**

```python
# Iceberg: Partition spec can evolve
CREATE TABLE iceberg.bronze.transactions (
    transaction_id STRING,
    timestamp TIMESTAMP,
    amount DECIMAL(18,2),
    -- ... other fields
)
USING iceberg
PARTITIONED BY (days(timestamp))  -- Hidden partitioning by day

-- No "partition column" in data, users query naturally:
SELECT * FROM iceberg.bronze.transactions
WHERE timestamp BETWEEN '2025-11-01' AND '2025-11-30';
-- Iceberg automatically prunes partitions (day-level granularity)

-- Later, change partitioning without rewriting data:
ALTER TABLE iceberg.bronze.transactions
SET PARTITION SPEC (months(timestamp));  -- Change to monthly partitions
```

```python
# Delta Lake: Physical partitioning (Hive-style)
CREATE TABLE delta.bronze.transactions (
    transaction_id STRING,
    timestamp TIMESTAMP,
    amount DECIMAL(18,2),
    date_partition STRING  -- Must be a physical column
)
USING delta
PARTITIONED BY (date_partition)

-- Users must include partition column in queries:
SELECT * FROM delta.bronze.transactions
WHERE date_partition BETWEEN '2025-11-01' AND '2025-11-30';

-- Cannot change partitioning later (requires full table rewrite)
```

**Why this matters:**
- **User Experience:** Analysts don't need to know partitioning scheme
- **Flexibility:** Can change partitioning as data grows (daily → monthly)
- **Query Optimization:** Iceberg auto-prunes partitions based on any timestamp filter

**4. Metadata Architecture (Performance at Scale)**

**Iceberg Metadata:**
```
Table Metadata (JSON)
├─ Current Snapshot ID: 123456
├─ Schema History: [v1, v2, v3]
└─ Partition Spec History: [v1, v2]

Snapshot 123456 (Avro)
├─ Manifest List: s3://lakehouse/.../snap-123456-m0.avro
└─ Summary: {added-files: 5, deleted-files: 0}

Manifest List (Avro)
├─ Manifest 1: partition=2025-11-19, added-files=3
└─ Manifest 2: partition=2025-11-20, added-files=2

Manifest (Avro)
├─ File 1: s3://lakehouse/.../00000-0-abc.parquet
│   ├─ Row count: 10,000
│   ├─ File size: 987 KB
│   └─ Column stats: {amount: {min: 10, max: 50000}}
└─ File 2: ...
```

**Advantages:**
- **Fast Metadata Lookups:** Manifest files are indexed by partition
- **Efficient Pruning:** Column stats enable predicate pushdown
- **Snapshot Isolation:** Readers see consistent snapshot, no locking

**Delta Lake Metadata:**
```
Delta Log (_delta_log/)
├─ 00000000000000000000.json (transaction 0)
├─ 00000000000000000001.json (transaction 1)
├─ 00000000000000000002.json (transaction 2)
└─ ...
└─ 00000000000000123456.json (transaction 123456)

# To read current state: Replay ALL transaction logs
# Checkpoint every 10 transactions to speed up (still slower than Iceberg)
```

**Disadvantages:**
- **Log Replay Overhead:** Must read many JSON files (mitigated by checkpoints)
- **Metadata Bloat:** Log grows indefinitely (requires VACUUM)

**At scale (1 million files):**
- Iceberg: ~100 manifest files (10K files each) → Fast lookups
- Delta: ~100K transaction logs (even with checkpoints) → Slower

**5. Multi-Table Transactions (Not Yet Used, But Future-Proof)**

```python
# Iceberg: Atomic commits across multiple tables
# Not directly supported in current API, but metadata allows it

# Use case: Update Silver + Gold in single transaction
# Ensures consistency across layers
```

**Delta Lake:** Supports multi-table transactions via Delta Live Tables (Databricks-only).

**Hudi:** Does not support multi-table transactions.

---

**Why Not Delta Lake?**

**Pros of Delta Lake:**
- Excellent Spark integration (optimized for Databricks)
- Change Data Feed (CDC built-in)
- Auto-optimization (OPTIMIZE, Z-ORDER)
- Great for Spark-centric pipelines

**Cons for Our Platform:**
- **Trino Support:** Limited (requires separate connector, not native)
- **Vendor Lock-In:** Best experience on Databricks (not vendor-neutral)
- **Partitioning:** Cannot change partition spec (our data grows, may need repartitioning)
- **Metadata Performance:** Log replay overhead at scale

**Why Not Hudi?**

**Pros of Hudi:**
- Excellent CDC support (built-in incremental pulls)
- DeltaStreamer for streaming ingestion
- Advanced indexing (Bloom filters, column stats)

**Cons for Our Platform:**
- **Complexity:** Steeper learning curve (Merge-on-Read vs. Copy-on-Write)
- **Query Performance:** Merge-on-Read adds overhead (slower reads)
- **Schema Evolution:** Limited (only add columns)
- **Trino Support:** Improving, but not as mature as Iceberg
- **Concurrency:** Timeline service required for writes (additional component)

---

**Iceberg Key Features We Use:**

**1. Snapshot Isolation & Time Travel**

```sql
-- Query current state
SELECT COUNT(*) FROM iceberg.bronze.transactions;
-- Result: 22,841

-- Query yesterday's state
SELECT COUNT(*) FROM iceberg.bronze.transactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2025-11-18 10:00:00';
-- Result: 18,320

-- Query specific snapshot
SELECT COUNT(*) FROM iceberg.bronze.transactions
FOR SYSTEM_VERSION AS OF 123455;

-- Use case: Compare data before/after bug fix
SELECT
    current.account_id,
    current.amount - old.amount AS amount_diff
FROM iceberg.bronze.transactions FOR SYSTEM_VERSION AS OF 123456 AS current
JOIN iceberg.bronze.transactions FOR SYSTEM_VERSION AS OF 123450 AS old
  ON current.transaction_id = old.transaction_id
WHERE current.amount != old.amount;
```

**2. ACID Guarantees (Exactly-Once Semantics)**

```python
# Spark Streaming writes are atomic
batch_df.writeTo("iceberg.bronze.transactions").append()

# Either:
# - Entire batch commits (new snapshot created)
# - OR batch fails (no partial writes, no dirty data)

# Concurrent readers see consistent snapshot (no locks, MVCC)
```

**3. Partitioning & Performance**

```sql
-- Partition pruning (automatic)
EXPLAIN SELECT * FROM iceberg.bronze.transactions
WHERE timestamp BETWEEN '2025-11-19 00:00:00' AND '2025-11-19 23:59:59';

-- Plan shows:
-- Partition filter: date_partition = 2025-11-19
-- Files scanned: 5 (out of 100 total)
-- Rows scanned: 10,000 (out of 22,841)
```

**4. Schema Evolution**

```sql
-- Add column (metadata-only, instant)
ALTER TABLE iceberg.bronze.transactions
ADD COLUMN new_field STRING;

-- Old Parquet files don't have new_field → Iceberg returns NULL
-- New writes include new_field
-- No data rewrite required
```

---

**Decision Matrix:**

| Criteria | Weight | Iceberg | Delta Lake | Hudi |
|----------|--------|---------|------------|------|
| **Trino Support** | ⭐⭐⭐⭐⭐ | 5/5 (Native) | 2/5 (Limited) | 3/5 (Improving) |
| **Vendor Neutrality** | ⭐⭐⭐⭐ | 5/5 | 3/5 | 5/5 |
| **Schema Evolution** | ⭐⭐⭐⭐ | 5/5 | 4/5 | 2/5 |
| **Partition Evolution** | ⭐⭐⭐ | 5/5 | 1/5 | 2/5 |
| **Metadata Performance** | ⭐⭐⭐⭐ | 5/5 | 3/5 | 4/5 |
| **Maturity** | ⭐⭐⭐ | 5/5 | 5/5 | 4/5 |
| **Community** | ⭐⭐⭐ | 5/5 | 5/5 | 4/5 |
| **CDC Support** | ⭐⭐ | 3/5 (Manual) | 5/5 (Built-in) | 5/5 (Built-in) |
| **Learning Curve** | ⭐⭐ | 4/5 | 5/5 | 2/5 |
| **Total Score** | | **4.6/5** | **3.4/5** | **3.5/5** |

**Verdict:** Iceberg is the best fit for a **multi-engine, vendor-neutral lakehouse platform** with **Trino as the primary query engine**.

---

### Q3: Describe how our platform handles exactly-once semantics in the streaming pipeline. What guarantees do we have at each stage?

**Answer:**

**Exactly-Once Semantics in Streaming: End-to-End Analysis**

**Overview: Three Stages of Delivery Guarantees**

```
Data Generator → Kafka → Spark Streaming → Iceberg
(At-least-once)  (Durable)  (Exactly-once)  (ACID)
```

**Stage-by-Stage Analysis:**

---

**Stage 1: Data Generator → Kafka (At-Least-Once)**

**Producer Configuration:**

```python
# generator.py
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka1:29092', 'kafka2:29092', 'kafka3:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    compression_type='snappy',
    acks='all',  # Wait for all in-sync replicas
    retries=10,  # Retry on transient failures
    max_in_flight_requests_per_connection=5,
    enable_idempotence=False  # ⚠️ Currently disabled
)

# Send message
future = producer.send('banking.transactions.raw', value=transaction)
future.get(timeout=10)  # Block until acknowledgment
```

**Guarantees:**
- `acks='all'`: Producer waits for all in-sync replicas (leader + followers) to acknowledge
- `retries=10`: Automatic retry on network errors or leader election
- **Delivery Guarantee:** **At-least-once** (message may be duplicated on retry)

**Failure Scenarios:**

| Scenario | Behavior | Result |
|----------|----------|--------|
| Producer sends, Kafka ACKs, producer receives ACK | Success | ✅ Exactly once |
| Producer sends, Kafka ACKs, **network drops ACK** | Producer retries | ❌ Duplicate (at-least-once) |
| Producer sends, Kafka fails before ACK | Producer retries | ❌ Possible duplicate |
| Producer sends, partition leader fails | Leader election, retry to new leader | ❌ Possible duplicate |

**To Achieve Exactly-Once (Not Implemented):**

```python
# Enable idempotent producer
producer = KafkaProducer(
    # ... other configs
    enable_idempotence=True,  # ✅ Enable idempotence
    transactional_id='data-generator-1',  # Required for transactions
    acks='all',
    retries=2147483647,  # Max retries
    max_in_flight_requests_per_connection=5
)

# Transactional send
producer.begin_transaction()
try:
    producer.send('banking.transactions.raw', value=transaction)
    producer.commit_transaction()
except Exception as e:
    producer.abort_transaction()
```

**How Idempotence Works:**
1. Producer gets unique Producer ID (PID) from Kafka
2. Each message has sequence number: `(PID, Partition, SequenceNumber)`
3. Kafka deduplicates messages with same `(PID, Partition, SequenceNumber)`
4. Retries send duplicate sequence numbers → Kafka ignores duplicates

**Why We Haven't Enabled It:**
- Simple data generator (low risk of duplicates)
- Deduplication happens in Silver layer (by `transaction_id`)
- Idempotence adds slight overhead (~5-10% throughput reduction)

**Recommendation:** Enable idempotence for production generators.

---

**Stage 2: Kafka Durability (Replication & Persistence)**

**Kafka Topic Configuration:**

```bash
Topic: banking.transactions.raw
Partitions: 3
Replication Factor: 2
Min In-Sync Replicas: 1
```

**Guarantees:**
- **Durability:** Messages are replicated to 2 brokers (leader + 1 follower)
- **Acknowledgment:** Producer receives ACK only after leader writes to log AND at least 1 follower replicates
- **Failure Tolerance:** Can lose 1 broker without data loss

**Kafka Persistence:**

```
Broker 1 (Leader for partition 0):
/var/lib/kafka/data/banking.transactions.raw-0/
├─ 00000000000000000000.log (segment 1)
├─ 00000000000000000000.index
├─ 00000000000000000000.timeindex
└─ 00000000000000010000.log (segment 2, after 10K messages)

Broker 2 (Follower for partition 0):
/var/lib/kafka/data/banking.transactions.raw-0/
└─ (same files, replicated)
```

**Guarantees:**
- **fsync:** Kafka flushes to disk periodically (`log.flush.interval.ms=1000`)
- **No data loss:** Even if all brokers crash, data persists on disk (Docker volumes)

**Failure Scenarios:**

| Scenario | Impact | Recovery |
|----------|--------|----------|
| Leader broker crashes | Follower promoted to leader | ✅ No data loss (replication) |
| Both leader and follower crash | Partition offline | ❌ Data unavailable (but not lost) |
| Disk corruption on leader | Unclean leader election | ⚠️ Potential data loss (unclean.leader.election.enable=false prevents this) |

**Current Configuration:**
```properties
# Prevent data loss from unclean leader election
unclean.leader.election.enable=false

# Wait for min.insync.replicas before ACK
min.insync.replicas=1

# Replication factor
default.replication.factor=2
```

**Trade-off:**
- **Higher Replication (RF=3, min.insync=2):** Better durability, but higher latency and storage
- **Our Setup (RF=2, min.insync=1):** Balanced (can lose 1 broker)

---

**Stage 3: Spark Streaming → Iceberg (Exactly-Once)**

**Spark Structured Streaming Configuration:**

```python
# kafka_to_iceberg_streaming.py

# Checkpoint location (critical for exactly-once)
CHECKPOINT_LOCATION = "s3a://lakehouse/checkpoints/bronze_transactions/"

# Read from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

# Write to Iceberg (foreachBatch pattern)
query = enriched_df \
    .writeStream \
    .foreachBatch(write_to_iceberg_bronze) \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()
```

**Exactly-Once Mechanism:**

**1. Checkpoint-Based Offset Management**

```
s3a://lakehouse/checkpoints/bronze_transactions/
├─ offsets/
│   ├─ 0   (batch 0: {partition 0: 0-999, partition 1: 0-999, partition 2: 0-999})
│   ├─ 1   (batch 1: {partition 0: 1000-1999, partition 1: 1000-1999, partition 2: 1000-1999})
│   └─ 2   (batch 2: ...)
├─ commits/
│   ├─ 0   (batch 0 successfully committed)
│   ├─ 1   (batch 1 successfully committed)
│   └─ 2   (batch 2 successfully committed)
└─ metadata
```

**Write-Ahead Log (WAL) Protocol:**

```
For each micro-batch:
1. Spark reads offsets from Kafka (e.g., 0-999 for each partition)
2. Writes offset range to checkpoint/offsets/<batch_id>
3. Processes batch (transform, filter, etc.)
4. Calls foreachBatch(write_to_iceberg_bronze)
5. Iceberg writes data files + creates new snapshot (ACID)
6. If successful, Spark writes checkpoint/commits/<batch_id>
7. If failure before step 6, batch is retried from step 1 (same offsets)
```

**Guarantees:**
- **Idempotent Reads:** Each batch reads the same offset range (deterministic)
- **Atomic Writes:** Iceberg commits are atomic (all-or-nothing)
- **Exactly-Once:** Offsets committed ONLY after successful Iceberg write

**2. Iceberg ACID Guarantees**

```python
def write_to_iceberg_bronze(batch_df, batch_id):
    # Iceberg writeTo() is atomic
    batch_df.writeTo("iceberg.bronze.transactions") \
        .option("write-format", "parquet") \
        .option("compression-codec", "gzip") \
        .append()

    # If this fails, Spark does NOT commit offsets
    # Next run will reprocess same batch (idempotent)
```

**Iceberg Commit Protocol:**

```
1. Spark writes Parquet files to MinIO (s3a://lakehouse/bronze/transactions/data/...)
2. Spark creates manifest file (lists new data files)
3. Spark creates snapshot metadata (references manifest)
4. Spark updates table metadata JSON (atomic file replacement)
   - Old: s3a://lakehouse/bronze/transactions/metadata/v123.metadata.json
   - New: s3a://lakehouse/bronze/transactions/metadata/v124.metadata.json
5. If step 4 fails, no snapshot is created (data files are orphaned, cleaned up later)
6. If step 4 succeeds, snapshot is committed (readers see new data)
```

**Atomicity:**
- **S3 PutObject** is atomic (either new metadata file exists or doesn't)
- No partial commits (all data files in snapshot or none)

**3. Failure Scenarios & Recovery**

| Failure Point | Spark Behavior | Iceberg State | Result |
|---------------|----------------|---------------|--------|
| **During Kafka read** | Retry batch | No change | ✅ Exactly-once (retry same offsets) |
| **During transformation** | Retry batch | No change | ✅ Exactly-once |
| **During Iceberg write (data files)** | Retry batch | Orphaned data files (no snapshot) | ✅ Exactly-once (files ignored) |
| **During Iceberg commit (metadata)** | Retry batch | Snapshot not created | ✅ Exactly-once |
| **After Iceberg commit, before checkpoint** | Retry batch | Duplicate snapshot | ❌ Duplicate data (fixed by dedup in Silver) |
| **After checkpoint** | Next batch | Success | ✅ Exactly-once |

**Critical Issue: Window of Duplication**

```
Timeline:
1. Batch 10: Process 10,000 records
2. Iceberg commit successful (snapshot created)
3. Spark executor crashes BEFORE writing checkpoint/commits/10
4. Spark restarts, sees no commit for batch 10
5. Reprocesses batch 10 (same 10,000 records)
6. Creates duplicate snapshot with duplicate records
```

**Mitigation:**
- **Silver Layer Deduplication:** `dropDuplicates(["transaction_id"])`
- **Iceberg Snapshot Expiration:** Old duplicate snapshots expire (configurable)
- **Probability:** Very low (checkpoint write is fast, ~10ms)

**To Eliminate Duplication (Not Implemented):**

```python
# Use Iceberg's "merge" instead of "append"
def write_to_iceberg_bronze(batch_df, batch_id):
    batch_df.writeTo("iceberg.bronze.transactions") \
        .using("iceberg") \
        .mergeKeys("transaction_id") \  # Dedup at write time
        .merge()
```

**Trade-off:**
- **Append (current):** Fast writes, duplicates handled in Silver
- **Merge:** Slower writes (read-modify-write), no duplicates

---

**Stage 4: End-to-End Exactly-Once (Bronze → Silver → Gold)**

**Silver Layer: Explicit Deduplication**

```python
# batch_aggregations.py
silver_df = bronze_df.dropDuplicates(["transaction_id"])
```

**Guarantees:**
- Even if Bronze has duplicates (Spark retry window), Silver deduplicates
- **Result:** Exactly-once in Silver and Gold layers

**Gold Layer: Idempotent Aggregations**

```python
# Overwrite partitions (idempotent)
daily_summary_df.writeTo("iceberg.gold.daily_account_summary") \
    .overwritePartitions()

# Multiple runs produce same result (idempotent)
```

---

**Summary: Delivery Guarantees**

| Component | Guarantee | Mechanism | Duplicates? |
|-----------|-----------|-----------|-------------|
| **Generator → Kafka** | At-least-once | Producer retries, acks=all | ⚠️ Yes (rare) |
| **Kafka Storage** | Durable | Replication (RF=2) | N/A |
| **Kafka → Spark** | Exactly-once* | Checkpointing + ACID | ⚠️ Rare (retry window) |
| **Spark → Iceberg (Bronze)** | Exactly-once* | ACID commits | ⚠️ Rare (retry window) |
| **Bronze → Silver** | Exactly-once | Deduplication by transaction_id | ✅ No |
| **Silver → Gold** | Exactly-once | Idempotent aggregations | ✅ No |

**\*Effectively exactly-once** with deduplication in Silver layer.

---

**Recommendations for True End-to-End Exactly-Once:**

1. **Enable Kafka Idempotent Producer:**
   ```python
   producer = KafkaProducer(enable_idempotence=True, transactional_id='gen-1')
   ```

2. **Use Iceberg Merge in Streaming:**
   ```python
   batch_df.writeTo("iceberg.bronze.transactions").mergeKeys("transaction_id").merge()
   ```

3. **Monitor Duplicate Rate:**
   ```sql
   -- Check for duplicates in Bronze
   SELECT transaction_id, COUNT(*)
   FROM iceberg.bronze.transactions
   GROUP BY transaction_id
   HAVING COUNT(*) > 1;
   ```

4. **Implement Dead Letter Queue (DLQ):**
   - On write failure, send to DLQ topic for manual review
   - Prevents data loss from permanent failures

---

## Query Engines & Performance

### Q4: Compare Trino and Spark SQL for querying our Iceberg tables. When would you use each, and what are the performance trade-offs?

**Answer:**

**Trino vs. Spark SQL: Comprehensive Comparison**

---

**Architecture Differences:**

**Trino (Presto-based):**
```
Coordinator (1 node)
├─ Query planning
├─ Task scheduling
└─ Result aggregation

Workers (N nodes)
├─ Data processing (in-memory)
├─ No intermediate storage
└─ Push-based execution
```

**Design Philosophy:**
- **Low-latency interactive queries:** Optimized for ad-hoc analytics
- **MPP (Massively Parallel Processing):** Distributed in-memory processing
- **No fault tolerance:** If a worker fails, query fails (must restart)
- **Cost-based optimizer:** Advanced query optimization

**Spark SQL:**
```
Driver (1 node)
├─ Query planning
├─ DAG scheduling
└─ Result collection

Executors (N nodes)
├─ Data processing (memory + disk)
├─ RDD/DataFrame transformations
└─ Pull-based execution (lazy)
```

**Design Philosophy:**
- **Batch processing:** Optimized for large-scale ETL
- **Fault tolerance:** Automatic task retry, checkpoint recovery
- **Disk spillover:** Can handle datasets larger than memory
- **Catalyst optimizer:** Rule-based + cost-based optimization

---

**Performance Comparison:**

**1. Query Latency (Interactive Queries)**

| Query Type | Trino | Spark SQL | Winner |
|------------|-------|-----------|--------|
| **Simple aggregation** (e.g., COUNT, SUM) | 100-500ms | 1-3s | **Trino** (10x faster) |
| **Complex joins** (multiple tables) | 1-5s | 3-10s | **Trino** (3x faster) |
| **Window functions** | 500ms-2s | 2-5s | **Trino** (2-3x faster) |
| **Full table scan** (small, 22K rows) | <1s | 1-2s | **Trino** (2x faster) |

**Why Trino is Faster for Interactive Queries:**

1. **No Overhead:** Direct execution (no DAG construction, no RDD overhead)
2. **In-Memory Shuffles:** All intermediate data in memory (no disk I/O)
3. **Pipeline Execution:** Streaming pipeline (rows flow through operators)
4. **Vectorized Processing:** Columnar processing (SIMD optimizations)

**Example: Simple Aggregation**

```sql
-- Query: Top 10 merchants by transaction volume
SELECT merchant, COUNT(*) as txn_count, SUM(amount) as total_amount
FROM iceberg.bronze.transactions
WHERE date_partition = '2025-11-19'
GROUP BY merchant
ORDER BY total_amount DESC
LIMIT 10;
```

**Trino Execution:**
```
1. Scan Parquet files (partition pruning: 1 day)
2. Column pruning (read only: merchant, amount)
3. Predicate pushdown (filter at file level)
4. Hash aggregation (distributed across workers)
5. Top-N sort (distributed, then final merge on coordinator)
6. Return results
Total time: ~300ms
```

**Spark SQL Execution:**
```
1. Build logical plan (Catalyst optimizer)
2. Build physical plan (choose join strategy, aggregation method)
3. Create RDD DAG (transformations)
4. Schedule tasks (driver → executors)
5. Execute stages:
   Stage 1: Scan Parquet, partial aggregation
   Stage 2: Shuffle aggregation
   Stage 3: Sort and limit
6. Collect results to driver
Total time: ~2s (overhead from DAG construction, task scheduling)
```

---

**2. Throughput (Batch Processing)**

| Workload | Trino | Spark SQL | Winner |
|----------|-------|-----------|--------|
| **Large ETL** (1 TB+ data) | Good (if fits in memory) | **Excellent** (disk spillover) | **Spark** |
| **Multi-stage pipeline** (Bronze→Silver→Gold) | Fair (no intermediate persistence) | **Excellent** (checkpoint, persist) | **Spark** |
| **Streaming** (continuous processing) | ❌ Not supported | ✅ Structured Streaming | **Spark** |
| **Iterative ML** (repeated passes over data) | ❌ Re-reads data each time | ✅ Caching, persist | **Spark** |

**Why Spark is Better for Batch:**

1. **Fault Tolerance:** Automatic retry on task failure (Trino query fails entirely)
2. **Disk Spillover:** Can process datasets larger than cluster memory
3. **Intermediate Persistence:** `.persist()`, `.cache()` for multi-stage pipelines
4. **Resource Management:** Dynamic allocation, better resource utilization

**Example: Large ETL (Bronze → Silver)**

**Trino Approach (Not Recommended):**
```sql
-- Single query to transform Bronze → Silver
INSERT INTO iceberg.silver.transactions
SELECT
    transaction_id,
    CAST(timestamp AS TIMESTAMP) as timestamp,
    CAST(amount AS DECIMAL(18,2)) as amount,
    -- ... 10 more transformations
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY timestamp DESC) as rn
    FROM iceberg.bronze.transactions
    WHERE date_partition >= '2025-11-01'
)
WHERE rn = 1  -- Deduplication
  AND amount > 0  -- Validation
  AND status IN ('COMPLETED', 'PENDING', 'FAILED');

-- Problems:
-- 1. No fault tolerance (if fails after 2 hours, start over)
-- 2. No intermediate checkpoints
-- 3. Memory pressure from large window function (ROW_NUMBER)
-- 4. Cannot easily debug/monitor progress
```

**Spark Approach (Recommended):**
```python
# Multi-stage with checkpointing
bronze_df = spark.table("iceberg.bronze.transactions") \
    .where("date_partition >= '2025-11-01'")

# Stage 1: Deduplication (checkpoint)
deduped_df = bronze_df.dropDuplicates(["transaction_id"]) \
    .persist()  # Cache in memory/disk

# Stage 2: Validation
validated_df = deduped_df \
    .filter(col("amount") > 0) \
    .filter(col("status").isin(["COMPLETED", "PENDING", "FAILED"]))

# Stage 3: Transformation
silver_df = validated_df \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .withColumn("amount", col("amount").cast("decimal(18,2)"))

# Stage 4: Write (atomic)
silver_df.writeTo("iceberg.silver.transactions").append()

# Benefits:
# - Fault tolerance: Each stage retries independently
# - Observability: Can inspect intermediate DataFrames
# - Resource optimization: persist() avoids recomputation
```

---

**3. Concurrency (Multiple Queries)**

| Scenario | Trino | Spark SQL | Winner |
|----------|-------|-----------|--------|
| **100 concurrent users** (BI dashboards) | **Excellent** (resource pooling) | Poor (resource contention) | **Trino** |
| **10 concurrent ETL jobs** | Poor (memory contention) | **Good** (dynamic allocation) | **Spark** |
| **Mixed workload** (BI + ETL) | **Good** (query queueing) | Fair | **Trino** |

**Trino Concurrency Management:**
```properties
# trino/etc/config.properties

# Query queueing (prevent OOM)
query.max-memory-per-node=2GB
query.max-total-memory-per-node=4GB

# Concurrent queries
query.max-concurrent-queries=20

# Resource groups (prioritize BI over ETL)
resource-groups.config-file=/etc/trino/resource-groups.json
```

**Resource Groups Example:**
```json
{
  "rootGroups": [
    {
      "name": "bi_queries",
      "softMemoryLimit": "50%",
      "hardConcurrencyLimit": 15,
      "maxQueued": 100
    },
    {
      "name": "etl_jobs",
      "softMemoryLimit": "50%",
      "hardConcurrencyLimit": 5,
      "maxQueued": 20
    }
  ]
}
```

**Spark Concurrency:**
```python
# Spark struggles with concurrent interactive queries
# Designed for sequential batch jobs

# Workaround: Spark Thrift Server (limited concurrency)
spark-submit --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
```

---

**4. Use Case Recommendations**

| Use Case | Recommended Engine | Reasoning |
|----------|-------------------|-----------|
| **BI Dashboards** (Superset, Tableau) | **Trino** | Low latency (<1s), high concurrency |
| **Ad-hoc Analytics** (data exploration) | **Trino** | Interactive, fast feedback |
| **Data Science Notebooks** (exploratory) | **Trino** or **Spark** | Trino for queries, Spark for ML |
| **ETL Pipelines** (Bronze→Silver→Gold) | **Spark** | Fault tolerance, multi-stage, persistence |
| **Streaming** (real-time ingestion) | **Spark Structured Streaming** | Trino doesn't support streaming |
| **Machine Learning** (training) | **Spark MLlib** | Iterative, caching, distributed ML |
| **Complex Joins** (>5 tables) | **Trino** | Advanced optimizer, broadcast joins |
| **Time Travel Queries** (Iceberg snapshots) | **Trino** or **Spark** | Both support `FOR SYSTEM_TIME AS OF` |
| **Large Table Scans** (1 TB+) | **Spark** | Disk spillover, fault tolerance |
| **OLAP Cubes** (pre-aggregated) | **Trino** | Fast, supports complex aggregations |

---

**5. Iceberg Integration Comparison**

| Feature | Trino | Spark SQL | Notes |
|---------|-------|-----------|-------|
| **Read Iceberg** | ✅ Native (`iceberg` connector) | ✅ Native (`iceberg` catalog) | Both excellent |
| **Write Iceberg** | ⚠️ Limited (INSERT, DELETE) | ✅ Full (INSERT, MERGE, UPDATE, DELETE) | Spark has richer API |
| **Schema Evolution** | ✅ Reads evolved schemas | ✅ Reads and writes | Spark can alter schemas |
| **Time Travel** | ✅ `FOR SYSTEM_TIME/VERSION AS OF` | ✅ `.option("snapshot-id", ...)` | Both support |
| **Partition Pruning** | ✅ Excellent | ✅ Excellent | Both leverage Iceberg metadata |
| **Predicate Pushdown** | ✅ Excellent (column stats) | ✅ Good | Trino slightly better |
| **Metadata Operations** | ⚠️ Limited (no VACUUM, EXPIRE) | ✅ Full (CALL procedures) | Spark has maintenance APIs |

**Example: Iceberg Maintenance**

**Trino:**
```sql
-- Read with time travel
SELECT * FROM iceberg.bronze.transactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2025-11-18 10:00:00';

-- Expire old snapshots (NOT SUPPORTED in Trino)
-- Must use Spark or Iceberg CLI
```

**Spark:**
```python
# Read with time travel
spark.read \
    .option("snapshot-id", 123456) \
    .table("iceberg.bronze.transactions")

# Expire old snapshots (older than 7 days)
spark.sql("""
    CALL iceberg.system.expire_snapshots(
        table => 'iceberg.bronze.transactions',
        older_than => TIMESTAMP '2025-11-12 00:00:00'
    )
""")

# Remove orphan files (cleanup failed writes)
spark.sql("""
    CALL iceberg.system.remove_orphan_files(
        table => 'iceberg.bronze.transactions'
    )
""")

# Rewrite small files (compaction)
spark.sql("""
    CALL iceberg.system.rewrite_data_files(
        table => 'iceberg.bronze.transactions',
        target_file_size_mb => 512
    )
""")
```

---

**6. Performance Tuning Comparison**

**Trino Tuning:**
```properties
# Memory
query.max-memory=10GB
query.max-memory-per-node=2GB

# Parallelism
task.concurrency=16  # Threads per task
task.max-worker-threads=128

# Optimizer
optimizer.join-reordering-strategy=AUTOMATIC
optimizer.use-cost-based-join-reordering=true

# Pushdown
iceberg.pushdown-filter-enabled=true
```

**Spark Tuning:**
```python
# Memory
spark.executor.memory=16g
spark.executor.memoryOverhead=2g
spark.memory.fraction=0.8

# Parallelism
spark.sql.shuffle.partitions=200  # Adjust based on data size
spark.default.parallelism=100

# Optimizer
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true

# Iceberg optimizations
spark.sql.iceberg.vectorization.enabled=true
spark.sql.iceberg.planning-mode=distributed  # Faster metadata scans
```

---

**7. Our Platform Usage**

**Current Setup:**
- **Trino:** Primary query engine for Superset dashboards, ad-hoc analytics
- **Spark SQL:** ETL pipelines (batch jobs), streaming ingestion

**Why This Split:**

**Trino for BI:**
```sql
-- Superset dashboard queries (executed via Trino)
-- Fast response for users (sub-second)

-- Example: Real-time transaction monitoring
SELECT
    DATE_TRUNC('hour', timestamp) as hour,
    COUNT(*) as transactions,
    SUM(amount) as volume
FROM iceberg.bronze.transactions
WHERE timestamp > NOW() - INTERVAL '24' HOUR
GROUP BY 1
ORDER BY 1 DESC;

-- Execution time: ~200ms (Trino)
-- vs. ~2-3s (Spark SQL)
```

**Spark for ETL:**
```python
# Batch job: Bronze → Silver → Gold
# Runs daily, processes millions of records
# Needs fault tolerance, multi-stage processing

# spark/jobs/batch_aggregations.py
# Execution time: 5-10 minutes
# Trino would struggle with fault tolerance, memory
```

---

**8. Decision Matrix**

**When to Use Trino:**
- ✅ Interactive queries (dashboards, reports)
- ✅ High concurrency (many users)
- ✅ Low latency requirements (<1s)
- ✅ Complex SQL (joins, window functions)
- ✅ Ad-hoc exploration
- ❌ Large ETL (>1 TB)
- ❌ Iterative ML workloads
- ❌ Streaming ingestion

**When to Use Spark SQL:**
- ✅ Large-scale ETL (batch processing)
- ✅ Streaming data ingestion
- ✅ Machine learning pipelines
- ✅ Multi-stage transformations
- ✅ Need fault tolerance
- ✅ Iterative algorithms
- ❌ Interactive queries (slow startup)
- ❌ High concurrency (resource contention)

---

**9. Hybrid Architecture (Best Practice)**

```
┌─────────────────────────────────────────────┐
│          Unified Iceberg Storage            │
│  (Bronze / Silver / Gold Tables)            │
└─────────────────────────────────────────────┘
           ↑                    ↑
           │                    │
    ┌──────┴──────┐      ┌─────┴──────┐
    │    Spark    │      │   Trino    │
    │   (Write)   │      │   (Read)   │
    └─────────────┘      └────────────┘
           │                    │
    ┌──────┴──────┐      ┌─────┴──────┐
    │ ETL Pipelines│      │ BI Dashboards│
    │  Streaming  │      │  Analytics  │
    └─────────────┘      └────────────┘
```

**Benefits:**
- **Best tool for each job:** Spark for writes, Trino for reads
- **Unified storage:** Both engines query same Iceberg tables
- **No data duplication:** Single source of truth
- **Optimized performance:** Each engine optimized for its use case

---

**10. Future Considerations**

**Emerging Technologies:**

1. **Apache Flink:**
   - Real-time streaming (lower latency than Spark)
   - Native Iceberg support
   - Use case: Replace Spark Streaming for <1s latency

2. **DuckDB:**
   - Embedded analytics (like SQLite)
   - Iceberg support (experimental)
   - Use case: Local development, edge analytics

3. **StarRocks/Apache Doris:**
   - OLAP databases with Iceberg integration
   - MPP architecture (like Trino)
   - Use case: Real-time analytics with sub-100ms latency

---

