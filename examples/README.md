# Examples - Hands-On Data Lakehouse Platform

Welcome to the hands-on examples for the streaming data lakehouse platform! 🚀

This directory contains practical examples to help you learn and experiment with the platform.

---

## 📚 Quick Start

### 1. Start the Platform

```bash
# From project root
make start

# Wait for all services to be ready (2-3 minutes)
make status

# Start data generation
make topics
make datagen-start

# Start streaming pipeline
make streaming-start

# Wait 2-3 minutes for data to accumulate
```

### 2. Verify Data is Flowing

```bash
# Connect to Trino
make shell-trino

# Check record count (should be > 100)
SELECT COUNT(*) FROM iceberg.bronze.transactions;

# Exit Trino
exit;
```

---

## 📖 Available Examples

### SQL Examples (for Data Analysts)

Located in `examples/sql/`

| File | Description | Difficulty | Time |
|------|-------------|------------|------|
| `01_basic_queries.sql` | Basic SELECT, aggregations, filtering | Beginner | 15 min |
| `02_intermediate_queries.sql` | Window functions, cohort analysis, trends | Intermediate | 30 min |

**How to use:**
```bash
# Open Trino CLI
make shell-trino

# Copy/paste queries from the files
# Or run specific query:
USE iceberg;
SELECT * FROM bronze.transactions LIMIT 10;
```

### Spark Examples (for Data Engineers)

Located in `examples/spark/`

| File | Description | Difficulty | Time |
|------|-------------|------------|------|
| `simple_etl.py` | Bronze → Silver transformation | Beginner | 5 min |
| `create_gold_tables.py` | Create aggregated Gold tables | Intermediate | 5 min |

**How to use:**

**Option 1: Using Helper Script (Easiest)**
```bash
# Make script executable
chmod +x examples/scripts/run_example.sh

# Run simple ETL
./examples/scripts/run_example.sh simple_etl

# Create Gold tables
./examples/scripts/run_example.sh create_gold
```

**Option 2: Manual Execution**
```bash
# Copy script to Spark container
docker cp examples/spark/simple_etl.py spark-master:/tmp/

# Submit Spark job
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  /tmp/simple_etl.py
```

---

## 🎯 Learning Paths

### Path 1: Data Analyst (SQL Focus)

**Goal:** Query and analyze transaction data

1. **Start Here:** `docs/HANDS_ON_TUTORIAL.md` → Use Case 1
2. **Run:** `sql/01_basic_queries.sql` (all queries)
3. **Try:** Top 5 merchants, transaction trends, fraud analysis
4. **Next:** `sql/02_intermediate_queries.sql`
5. **Build:** Create your own Superset dashboard

**Time:** 1 hour

### Path 2: Data Engineer (Pipeline Builder)

**Goal:** Build ETL pipelines

1. **Start Here:** `docs/HANDS_ON_TUTORIAL.md` → Use Case 3
2. **Run:** `spark/simple_etl.py` (Bronze → Silver)
3. **Examine:** Check Silver data quality in Trino
4. **Run:** `spark/create_gold_tables.py` (Silver → Gold)
5. **Verify:** Query Gold tables, measure performance

**Time:** 45 minutes

### Path 3: Platform Engineer (Advanced Features)

**Goal:** Learn Iceberg time travel, schema evolution

1. **Start Here:** `docs/HANDS_ON_TUTORIAL.md` → Use Case 6
2. **Explore:** Iceberg snapshots and metadata
3. **Practice:** Time travel queries
4. **Try:** Schema evolution (add columns)
5. **Experiment:** Rollback to previous snapshot

**Time:** 30 minutes

### Path 4: Full Stack (Complete Journey)

**Goal:** End-to-end understanding

1. Complete Path 1 (Analyst)
2. Complete Path 2 (Engineer)
3. Complete Path 3 (Platform)
4. **Build:** Custom use case (your own business problem)

**Time:** 3-4 hours

---

## 🏃 Quick Examples

### Example 1: Your First Query (30 seconds)

```bash
make shell-trino
```

```sql
USE iceberg;
SELECT
    merchant,
    COUNT(*) as transactions,
    ROUND(SUM(amount), 2) as revenue
FROM bronze.transactions
GROUP BY merchant
ORDER BY revenue DESC
LIMIT 5;
```

### Example 2: Run ETL Pipeline (5 minutes)

```bash
./examples/scripts/run_example.sh simple_etl
```

Wait for completion, then verify:

```bash
make shell-trino
```

```sql
USE iceberg;
SELECT COUNT(*) FROM silver.transactions;
```

### Example 3: Create Dashboard (10 minutes)

1. Open Superset: http://localhost:8088 (admin/admin)
2. Add Trino database connection
3. Go to SQL Lab
4. Run query from `sql/01_basic_queries.sql`
5. Click "Create Chart"
6. Create visualizations!

---

## 🔍 Example Scenarios

### Scenario 1: Find Fraudulent Transactions

```sql
-- In Trino CLI
USE iceberg.bronze;

SELECT
    transaction_id,
    account_id,
    merchant,
    amount,
    CAST(timestamp AS TIMESTAMP) as time,
    location.city
FROM transactions
WHERE is_fraud = true
ORDER BY amount DESC
LIMIT 20;
```

### Scenario 2: Identify Top Customers

```sql
SELECT
    account_id,
    customer_name,
    COUNT(*) as transaction_count,
    ROUND(SUM(amount), 2) as total_spent,
    ROUND(AVG(amount), 2) as avg_per_transaction
FROM iceberg.bronze.transactions
GROUP BY account_id, customer_name
HAVING COUNT(*) > 5
ORDER BY total_spent DESC
LIMIT 10;
```

### Scenario 3: Hourly Transaction Pattern

```sql
SELECT
    HOUR(CAST(timestamp AS TIMESTAMP)) as hour,
    COUNT(*) as transactions,
    ROUND(AVG(amount), 2) as avg_amount
FROM iceberg.bronze.transactions
GROUP BY HOUR(CAST(timestamp AS TIMESTAMP))
ORDER BY hour;
```

---

## 🛠️ Troubleshooting

### Issue: "No data in Bronze table"

**Solution:**
```bash
# Check if data generator is running
docker ps | grep data-generator

# If not running, start it
make datagen-start

# Wait 1-2 minutes, then check again
make shell-trino
SELECT COUNT(*) FROM iceberg.bronze.transactions;
```

### Issue: "Spark job fails"

**Solution:**
```bash
# Check Spark cluster is healthy
make status

# View Spark logs
docker logs spark-master --tail 50

# Restart Spark if needed
docker restart spark-master spark-worker-1 spark-worker-2
```

### Issue: "Can't connect to Trino"

**Solution:**
```bash
# Check Trino is running
docker ps | grep trino

# Restart Trino
docker restart trino

# Wait 30 seconds, then try again
make shell-trino
```

---

## 💡 Tips & Tricks

### Tip 1: Monitor Data Ingestion

```bash
# Watch data generator logs
docker logs -f data-generator

# Watch Spark streaming logs
docker logs -f spark-master | grep "Batch"

# Check data freshness in Trino
make shell-trino
SELECT MAX(ingestion_timestamp) FROM iceberg.bronze.transactions;
```

### Tip 2: Query Performance

```sql
-- Use partition filters for better performance
-- GOOD (uses partition pruning):
SELECT COUNT(*) FROM iceberg.bronze.transactions
WHERE date_partition = CURRENT_DATE;

-- BAD (full table scan):
SELECT COUNT(*) FROM iceberg.bronze.transactions
WHERE CAST(timestamp AS DATE) = CURRENT_DATE;
```

### Tip 3: Explore Data Before Querying

```sql
-- Check table structure
DESCRIBE iceberg.bronze.transactions;

-- Get data statistics
SELECT
    COUNT(*) as total_records,
    MIN(CAST(timestamp AS DATE)) as earliest_date,
    MAX(CAST(timestamp AS DATE)) as latest_date,
    COUNT(DISTINCT merchant) as unique_merchants
FROM iceberg.bronze.transactions;
```

---

## 📊 Expected Results

After running the examples, you should have:

### Tables Created

- ✅ `iceberg.bronze.transactions` - Raw streaming data (~500-1000 records)
- ✅ `iceberg.silver.transactions` - Cleaned, validated data
- ✅ `iceberg.gold.daily_account_summary` - Account aggregations
- ✅ `iceberg.gold.merchant_performance` - Merchant metrics
- ✅ `iceberg.gold.hourly_trends` - Time-based trends
- ✅ `iceberg.gold.geographic_summary` - Location analytics

### Skills Acquired

- ✅ Querying streaming data with Trino SQL
- ✅ Building ETL pipelines with Spark
- ✅ Understanding Bronze/Silver/Gold architecture
- ✅ Data quality validation
- ✅ Creating business aggregations
- ✅ Real-time data monitoring

---

## 🎓 Next Steps

1. **Build Your Own Use Case**
   - Define a business problem
   - Design your Silver/Gold schema
   - Implement transformations
   - Create dashboards in Superset

2. **Integrate External Data**
   - Add CSV file ingestion
   - Connect to external database
   - Implement CDC (Change Data Capture)

3. **Advanced Topics**
   - Time travel and audit trails
   - Schema evolution
   - Performance tuning (compaction)
   - Real-time alerting

4. **Scale the Platform**
   - Add Kafka partitions
   - Increase Spark workers
   - Optimize for larger datasets

---

## 📚 Additional Resources

- **Full Tutorial:** `docs/HANDS_ON_TUTORIAL.md`
- **Infrastructure Review:** `docs/INFRASTRUCTURE_REVIEW.md`
- **Interview Prep:** `docs/interviews/`
- **Iceberg Docs:** https://iceberg.apache.org/
- **Trino Docs:** https://trino.io/docs/current/
- **Spark Docs:** https://spark.apache.org/docs/latest/

---

## 🆘 Getting Help

**Stuck? Try these:**

1. Check logs: `make logs-<service>`
2. Restart services: `make restart`
3. View service status: `make status`
4. Check documentation: `docs/`

**Still stuck?**
- Review `docs/HANDS_ON_TUTORIAL.md` → Troubleshooting section
- Check `INFRASTRUCTURE_REVIEW.md` → Operational Procedures

---

**🎉 Happy Learning!**

Start with the basics, experiment freely, and build something awesome! 🚀
