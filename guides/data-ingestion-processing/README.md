# Data Ingestion and Processing Guide

Complete resources for streaming data ingestion, processing, and analytics in the lakehouse platform.

## 📚 Overview

This directory contains all documentation, scripts, and examples for working with data streams in the lakehouse platform. Whether you're a beginner or experienced user, you'll find step-by-step guides and ready-to-use tools here.

## 📁 Directory Structure

```
guides/data-ingestion-processing/
├── README.md                          # This file - main index
├── docs/                              # Detailed documentation
│   ├── BEGINNER_WALKTHROUGH.md       # Step-by-step guide for beginners
│   └── DATA_STREAMING_GUIDE.md       # Advanced streaming concepts
├── scripts/                           # Automation scripts
│   ├── start-pipeline.sh             # Start complete data pipeline
│   └── monitor-pipeline.sh           # Monitor pipeline status
└── examples/                          # Code examples
    ├── custom-producer.py            # Send custom data to Kafka
    └── trino-queries.sql             # SQL query examples
```

---

## 🚀 Quick Start

### For Complete Beginners

If you have **little to no experience** with Kafka or data streaming:

1. **Read first**: [`docs/BEGINNER_WALKTHROUGH.md`](docs/BEGINNER_WALKTHROUGH.md)
   - Step-by-step instructions with every command explained
   - Expected outputs shown for each step
   - Troubleshooting for common issues
   - Explains what's happening behind the scenes

2. **Use the automated script**:
   ```bash
   # Start the entire pipeline automatically
   bash guides/data-ingestion-processing/scripts/start-pipeline.sh
   ```

3. **Monitor the pipeline**:
   ```bash
   # View real-time status
   bash guides/data-ingestion-processing/scripts/monitor-pipeline.sh
   ```

### For Experienced Users

If you're **familiar with streaming platforms**:

1. **Read**: [`docs/DATA_STREAMING_GUIDE.md`](docs/DATA_STREAMING_GUIDE.md)
   - Deep dive into architecture
   - Advanced Spark streaming configuration
   - Schema evolution and partition strategies
   - Performance optimization

2. **Customize**: Use examples as templates
   - [`examples/custom-producer.py`](examples/custom-producer.py) - Send your own data
   - [`examples/trino-queries.sql`](examples/trino-queries.sql) - Advanced SQL analytics

---

## 📖 Documentation

### [BEGINNER_WALKTHROUGH.md](docs/BEGINNER_WALKTHROUGH.md)

**Perfect for**: First-time users, learning Kafka, understanding data pipelines

**What's inside**:
- ✅ Pre-flight checks (verify system is ready)
- ✅ Starting services step-by-step
- ✅ Generating fake data (banking transactions)
- ✅ Viewing data flow through Kafka
- ✅ Processing with Spark Structured Streaming
- ✅ Querying data with Trino SQL
- ✅ Monitoring the complete pipeline
- ✅ Troubleshooting common issues
- ✅ Hands-on experiments
- ✅ "What's actually happening?" explanations

**Start here if**:
- This is your first time working with streaming data
- You want to understand each step in detail
- You need commands with expected outputs

### [DATA_STREAMING_GUIDE.md](docs/DATA_STREAMING_GUIDE.md)

**Perfect for**: Data engineers, advanced users, production planning

**What's inside**:
- 🔧 Data streaming pipeline architecture
- 🔧 Kafka topic configuration and management
- 🔧 Spark Structured Streaming deep dive
- 🔧 Creating Iceberg tables (Bronze/Silver/Gold)
- 🔧 Schema and partition evolution
- 🔧 Time travel queries
- 🔧 Advanced analytics patterns
- 🔧 Performance tuning
- 🔧 Table maintenance operations

**Start here if**:
- You understand streaming basics
- You want to customize the pipeline
- You need performance optimization
- You're planning production deployment

---

## 🛠️ Scripts

### [start-pipeline.sh](scripts/start-pipeline.sh)

**Automated pipeline startup script**

**What it does**:
1. ✅ Verifies all prerequisites (Docker, services)
2. ✅ Creates Kafka topics if needed
3. ✅ Starts data generator
4. ✅ Waits for data to flow
5. ✅ Submits Spark streaming job
6. ✅ Waits for first batch processing
7. ✅ Verifies data in Iceberg tables
8. ✅ Provides next steps and monitoring links

**Usage**:
```bash
# Navigate to project root first
cd /home/user/onprem-streaming-processing-system

# Run the script
bash guides/data-ingestion-processing/scripts/start-pipeline.sh
```

**Output**: Color-coded status messages with ✓/✗ indicators

**Time**: ~2-3 minutes for complete setup

### [monitor-pipeline.sh](scripts/monitor-pipeline.sh)

**Real-time pipeline monitoring dashboard**

**What it shows**:
- ✅ Service health (all containers)
- ✅ Data generator status
- ✅ Kafka topic message counts
- ✅ Spark streaming job status
- ✅ Iceberg table record counts
- ✅ Data freshness/lag
- ✅ Processing rate statistics
- ✅ Quick access links to UIs

**Usage**:
```bash
# One-time check
bash guides/data-ingestion-processing/scripts/monitor-pipeline.sh

# Continuous monitoring (refreshes every 5 seconds)
watch -n 5 bash guides/data-ingestion-processing/scripts/monitor-pipeline.sh
```

**Tip**: Run this in a separate terminal while the pipeline is running

---

## 💡 Examples

### [custom-producer.py](examples/custom-producer.py)

**Python script for sending custom data to Kafka**

**Features**:
- ✅ Single transaction example
- ✅ Batch transaction example
- ✅ Proper error handling
- ✅ Delivery confirmation
- ✅ Configurable connection settings

**Usage**:
```bash
# Install dependencies first
pip install kafka-python

# Run the example
python3 guides/data-ingestion-processing/examples/custom-producer.py
```

**Customize**:
```python
# Edit the transaction structure
transaction = {
    'transaction_id': 'your-id',
    'amount': 999.99,
    'custom_field': 'your-value',
    # ... add your fields
}
```

**Output**:
- Shows successful message delivery
- Displays Kafka topic, partition, offset
- Provides Trino query to verify data

### [trino-queries.sql](examples/trino-queries.sql)

**Comprehensive SQL query examples for Trino**

**Categories**:
1. **Basic Exploration** - SHOW, DESCRIBE, table discovery
2. **Simple Queries** - SELECT, COUNT, ORDER BY
3. **Aggregations** - GROUP BY, SUM, AVG, analytics
4. **Filtering** - WHERE, complex predicates
5. **Advanced Analytics** - Percentiles, moving averages
6. **Table Creation** - CTAS, Silver/Gold layers
7. **Time Travel** - Historical queries, snapshots
8. **Metadata Queries** - Files, partitions, manifests
9. **Performance** - Optimization, EXPLAIN plans
10. **Data Quality** - Null checks, duplicates
11. **Incremental Updates** - INSERT, DELETE, MERGE patterns
12. **Views** - CREATE VIEW examples

**Usage**:
```bash
# Connect to Trino
make shell-trino

# Copy and paste queries from the file
# Or run specific query:
cat guides/data-ingestion-processing/examples/trino-queries.sql | \
  grep -A 5 "Count total transactions"
```

---

## 🔄 Complete Workflow

### End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. DATA GENERATION                                           │
│    ┌─────────────┐                                          │
│    │  Generator  │ Creates fake banking transactions        │
│    │  (Python)   │ Rate: 1 transaction/second               │
│    └──────┬──────┘                                          │
└───────────┼──────────────────────────────────────────────────┘
            │ Produces JSON messages
            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. MESSAGE STREAMING                                         │
│    ┌─────────────┐                                          │
│    │    Kafka    │ Receives and distributes messages        │
│    │ (3 brokers) │ Topic: banking.transactions.raw          │
│    └──────┬──────┘ Partitions: 3, Replication: 2            │
└───────────┼──────────────────────────────────────────────────┘
            │ Streams continuously
            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. STREAM PROCESSING                                         │
│    ┌─────────────┐                                          │
│    │    Spark    │ Reads micro-batches (every 30 sec)       │
│    │  Streaming  │ Parses JSON, validates, enriches         │
│    └──────┬──────┘                                          │
└───────────┼──────────────────────────────────────────────────┘
            │ Writes with ACID guarantees
            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. DATA LAKE STORAGE                                         │
│    ┌─────────────┐                                          │
│    │   Iceberg   │ ACID transactions on object storage      │
│    │   Tables    │ Format: Parquet (columnar)               │
│    │  (MinIO)    │ Partitioned by: date                     │
│    └──────┬──────┘                                          │
└───────────┼──────────────────────────────────────────────────┘
            │ Registered in
            ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. METADATA CATALOG                                          │
│    ┌─────────────┐                                          │
│    │    Hive     │ Tracks table schemas and locations       │
│    │  Metastore  │ Backend: PostgreSQL                      │
│    └──────┬──────┘                                          │
└───────────┼──────────────────────────────────────────────────┘
            │ Queried by
            ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. SQL ANALYTICS                                             │
│    ┌─────────────┐                                          │
│    │    Trino    │ Distributed SQL query engine             │
│    │             │ Direct queries on data lake              │
│    └─────────────┘ No ETL needed!                           │
└─────────────────────────────────────────────────────────────┘
```

### Typical User Journey

**1. Start the Platform**
```bash
# Start all services
make start

# Or use the automated script
bash guides/data-ingestion-processing/scripts/start-pipeline.sh
```

**2. Monitor Data Flow**
```bash
# Terminal 1: Monitor overall status
bash guides/data-ingestion-processing/scripts/monitor-pipeline.sh

# Terminal 2: Watch data generator
make logs-datagen

# Browser: Open Kafka UI
open http://localhost:8080
```

**3. Query Your Data**
```bash
# Connect to Trino
make shell-trino

# Run analytics
SELECT COUNT(*) FROM iceberg.bronze.transactions;
```

**4. Create Business Views**
```sql
-- Create Silver layer (cleaned data)
CREATE TABLE iceberg.silver.validated_transactions AS
SELECT * FROM iceberg.bronze.transactions
WHERE amount > 0 AND status = 'PENDING';

-- Create Gold layer (aggregates)
CREATE TABLE iceberg.gold.daily_summary AS
SELECT
    date_partition,
    transaction_type,
    COUNT(*) as txn_count,
    SUM(amount) as total_amount
FROM iceberg.silver.validated_transactions
GROUP BY date_partition, transaction_type;
```

**5. Send Custom Data** (Optional)
```bash
# Use the custom producer
python3 guides/data-ingestion-processing/examples/custom-producer.py

# Verify it arrived
make shell-trino
> SELECT * FROM iceberg.bronze.transactions WHERE account_id = 'ACC9999';
```

---

## 📊 Monitoring & Observability

### Web Interfaces

| Interface | URL | Purpose |
|-----------|-----|---------|
| **Kafka UI** | http://localhost:8080 | View topics, messages, consumer lag |
| **Spark UI** | http://localhost:8888 | Monitor streaming jobs, executors |
| **Trino UI** | http://localhost:8086 | Query history, cluster status |
| **MinIO Console** | http://localhost:9001 | Browse stored data files |
| **Grafana** | http://localhost:3000 | System metrics, dashboards |

### Command-Line Monitoring

```bash
# Service status
make status

# View logs
make logs-datagen      # Data generator
make logs-kafka        # Kafka brokers
make logs-spark        # Spark cluster
make logs-trino        # Trino engine

# Test services
make test-all          # Test all services
make test-kafka        # Test Kafka only
make test-trino        # Test Trino only
```

### Key Metrics to Watch

1. **Data Generator**: Transactions/second produced
2. **Kafka**: Message count, consumer lag
3. **Spark**: Batch processing time, scheduling delay
4. **Iceberg**: Record count, data freshness
5. **System**: CPU, memory, disk usage

---

## 🎓 Learning Path

### Beginner Path (New to Streaming)

1. **Read**: [`docs/BEGINNER_WALKTHROUGH.md`](docs/BEGINNER_WALKTHROUGH.md) (30 min)
2. **Do**: Run the automated setup script (5 min)
3. **Explore**: Open Kafka UI and watch messages flow (10 min)
4. **Query**: Connect to Trino and run basic queries (15 min)
5. **Experiment**: Try the custom producer example (15 min)

**Total time**: ~75 minutes to full understanding

### Intermediate Path (Some Experience)

1. **Skim**: [`docs/BEGINNER_WALKTHROUGH.md`](docs/BEGINNER_WALKTHROUGH.md) (10 min)
2. **Read**: [`docs/DATA_STREAMING_GUIDE.md`](docs/DATA_STREAMING_GUIDE.md) sections 1-4 (30 min)
3. **Do**: Manual setup to understand each component (20 min)
4. **Customize**: Modify data generator or Spark job (30 min)
5. **Optimize**: Create Silver/Gold tables (20 min)

**Total time**: ~110 minutes to advanced usage

### Advanced Path (Production Planning)

1. **Read**: Full [`docs/DATA_STREAMING_GUIDE.md`](docs/DATA_STREAMING_GUIDE.md) (60 min)
2. **Review**: Architecture decisions in main ARCHITECTURE.md (30 min)
3. **Plan**: Schema design for your use case (60 min)
4. **Implement**: Custom Spark jobs for your data (varies)
5. **Optimize**: Performance tuning, monitoring setup (60 min)

**Total time**: ~210+ minutes to production-ready

---

## 🔧 Customization Guide

### Change Data Generation Rate

```bash
# Edit .env file
nano .env

# Change this line:
GENERATION_INTERVAL_MS=500  # 2 transactions/second (default: 1000)

# Restart generator
docker compose restart data-generator
```

### Change Spark Batch Interval

Edit `spark/jobs/kafka_to_iceberg_streaming.py`:
```python
# Change from 30 seconds to 10 seconds
.trigger(processingTime='10 seconds')
```

### Add Custom Fields to Data

Edit `data-generator/generator.py`:
```python
transaction = {
    # Existing fields...
    'custom_field_1': 'your_value',
    'custom_field_2': 123,
}
```

Update Spark schema in `kafka_to_iceberg_streaming.py`:
```python
StructField("custom_field_1", StringType(), True),
StructField("custom_field_2", IntegerType(), True),
```

### Create Custom Topics

```bash
docker exec kafka-1 kafka-topics --create \
  --topic your-custom-topic \
  --bootstrap-server kafka-1:29092 \
  --partitions 3 \
  --replication-factor 2
```

---

## ❓ Troubleshooting

### Quick Diagnostics

```bash
# 1. Check all services
make status

# 2. Run automated monitor
bash guides/data-ingestion-processing/scripts/monitor-pipeline.sh

# 3. Check specific logs
make logs-datagen
make logs-spark
```

### Common Issues

| Issue | Solution |
|-------|----------|
| No data in Kafka | Check generator: `make logs-datagen` |
| Spark job not running | Check Spark UI: http://localhost:8888 |
| No tables in Trino | Wait 60 sec for first batch, check Spark logs |
| Data not updating | Verify consumer lag in Kafka UI |
| Out of memory | Use lightweight mode: `docker-compose.lightweight.yml` |

### Get Help

- **Documentation**: See main README.md and ARCHITECTURE.md
- **Logs**: `make logs` or `docker compose logs <service>`
- **Status**: `make status` or scripts/monitor-pipeline.sh
- **Test**: `make test-all` to verify all components

---

## 📝 Best Practices

### Data Ingestion

✅ **DO**:
- Use appropriate Kafka topic partitions (3-6 for this setup)
- Set replication factor ≥ 2 for fault tolerance
- Validate data before sending to Kafka
- Use meaningful message keys for partitioning
- Monitor consumer lag

❌ **DON'T**:
- Don't send malformed JSON to Kafka
- Don't skip schema validation
- Don't ignore consumer lag warnings
- Don't use topic names with special characters

### Stream Processing

✅ **DO**:
- Process in micro-batches (30-60 sec intervals)
- Use checkpointing for fault tolerance
- Partition Iceberg tables by date
- Monitor Spark batch processing time
- Handle schema evolution gracefully

❌ **DON'T**:
- Don't set batch intervals < 10 seconds
- Don't skip checkpoint configuration
- Don't create too many small files
- Don't ignore processing delays

### Data Querying

✅ **DO**:
- Use partition pruning in WHERE clauses
- Create materialized views for common queries
- Use LIMIT when exploring large tables
- Monitor query performance with EXPLAIN
- Leverage Iceberg time travel for audits

❌ **DON'T**:
- Don't query without WHERE on large tables
- Don't use SELECT * in production queries
- Don't ignore data freshness checks
- Don't skip indexes/partitions

---

## 🚀 Next Steps

After mastering the basics:

1. **Explore Advanced Topics**: Read DATA_STREAMING_GUIDE.md sections 5-6
2. **Build Custom Pipelines**: Modify Spark jobs for your data
3. **Implement Data Quality**: Add validation and testing
4. **Create Dashboards**: Build Grafana dashboards for metrics
5. **Plan Production**: Review security and scaling in ARCHITECTURE.md

---

## 📚 Additional Resources

### Internal Documentation
- [ARCHITECTURE.md](../../ARCHITECTURE.md) - Platform architecture
- [README.md](../../README.md) - Project overview
- [QUICK_START.md](../../QUICK_START.md) - Quick setup guide

### External Resources
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Trino Documentation](https://trino.io/docs/current/)

---

## 💬 Feedback

This guide is continually improved based on user feedback. If you:
- Find errors or outdated information
- Have suggestions for improvements
- Need additional examples
- Want to contribute new scripts

Please create an issue or submit a pull request!

---

**Happy Streaming!** 🎉

Last Updated: 2024-01-16
