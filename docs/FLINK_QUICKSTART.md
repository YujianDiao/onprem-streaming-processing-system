# Flink Quick Start Guide

Get Apache Flink up and running with banking pipelines in 5 minutes!

## Prerequisites

- Docker and Docker Compose installed
- At least 16GB RAM available
- Kafka cluster running (from the main platform)

## Step 1: Start Flink Cluster

```bash
# Start Flink JobManager and TaskManagers
docker-compose up -d flink-jobmanager flink-taskmanager-1 flink-taskmanager-2

# Verify services are running
docker-compose ps | grep flink
```

Expected output:
```
flink-jobmanager       running    0.0.0.0:6123->6123/tcp, 0.0.0.0:8081->8081/tcp
flink-taskmanager-1    running    0.0.0.0:6121-6122->6121-6122/tcp
flink-taskmanager-2    running    0.0.0.0:6131-6132->6121-6122/tcp
```

## Step 2: Access Flink Dashboard

Open your browser: **http://localhost:8081**

You should see:
- JobManager status: Running
- TaskManagers: 2 available
- Total task slots: 8

## Step 3: Create Kafka Topics for Flink Outputs

```bash
bash scripts/setup-flink-topics.sh
```

This creates:
- `banking.fraud.alerts` - Fraud detection alerts
- `banking.monitoring.alerts` - Transaction monitoring alerts
- `banking.metrics.realtime` - Real-time KPIs
- `banking.balances.realtime` - Live account balances
- `banking.balance.alerts` - Balance alerts
- `banking.balance.snapshots` - Hourly snapshots

## Step 4: Build and Submit Flink Jobs

```bash
cd flink
./build-and-submit.sh all
```

This will:
1. Build the Flink job JAR (takes 2-3 minutes first time)
2. Submit all three banking jobs:
   - Fraud Detection Job
   - Transaction Monitoring Job
   - Balance Aggregation Job

## Step 5: Verify Jobs Are Running

**In Flink Dashboard (http://localhost:8081):**
- Click "Running Jobs" in left menu
- You should see 3 running jobs
- Click on each job to see:
  - Job graph (visual representation)
  - Task metrics
  - Checkpoints

**Via CLI:**
```bash
docker exec flink-jobmanager /opt/flink/bin/flink list -r
```

## Step 6: View Real-time Outputs

### Monitor Fraud Alerts

```bash
docker exec kafka-1 kafka-console-consumer \
    --bootstrap-server kafka-1:29092 \
    --topic banking.fraud.alerts \
    --from-beginning
```

You should see fraud alerts like:
```json
{
  "transactionId": "abc-123",
  "accountId": "ACC1234",
  "fraudType": "RULE_BASED",
  "reason": "High: Transaction amount exceeds $10,000",
  "severity": "HIGH",
  "amount": 12500.50,
  "detectedAt": 1699999999999
}
```

### Monitor Real-time Balances

```bash
docker exec kafka-1 kafka-console-consumer \
    --bootstrap-server kafka-1:29092 \
    --topic banking.balances.realtime \
    --from-beginning
```

### Monitor Transaction Alerts

```bash
docker exec kafka-1 kafka-console-consumer \
    --bootstrap-server kafka-1:29092 \
    --topic banking.monitoring.alerts \
    --from-beginning
```

## Step 7: Generate Test Transactions

If you haven't started the data generator yet:

```bash
docker-compose up -d data-generator
```

This generates realistic banking transactions every 2 seconds, which will trigger:
- Fraud detection alerts (2% of transactions)
- Balance updates (every transaction)
- Volume alerts (if >20 transactions/hour per account)

## Troubleshooting

### Jobs not showing in Flink UI

**Problem:** Submitted jobs but don't see them in dashboard

**Solution:**
```bash
# Check JobManager logs
docker-compose logs flink-jobmanager

# Check if jobs were submitted
docker exec flink-jobmanager ls -l /opt/flink-jobs/target/

# Rebuild and resubmit
cd flink && ./build-and-submit.sh all
```

### Build Failures

**Problem:** Maven build fails

**Solution:**
```bash
# Check if Maven is available in container
docker exec flink-jobmanager mvn --version

# Clean Maven cache and rebuild
docker exec flink-jobmanager bash -c "cd /opt/flink-jobs && mvn clean install -U"
```

### No Kafka Messages

**Problem:** No messages appearing in Kafka topics

**Solution:**
```bash
# 1. Verify data generator is running
docker-compose ps data-generator

# 2. Check if transactions are being produced
docker exec kafka-1 kafka-console-consumer \
    --bootstrap-server kafka-1:29092 \
    --topic banking.transactions.raw \
    --max-messages 5

# 3. Check Flink job logs
docker-compose logs flink-taskmanager-1 | grep -i error
```

### TaskManager not connecting

**Problem:** TaskManagers show as unavailable in Flink UI

**Solution:**
```bash
# Restart TaskManagers
docker-compose restart flink-taskmanager-1 flink-taskmanager-2

# Check network connectivity
docker exec flink-taskmanager-1 ping flink-jobmanager

# Check logs
docker-compose logs flink-taskmanager-1
```

## Next Steps

### 1. Explore Flink Dashboard

Navigate to http://localhost:8081 and explore:
- **Jobs Tab**: See running jobs, job graphs, metrics
- **Task Managers**: View resource utilization, heap usage
- **Job Manager**: Check configuration
- **Completed Jobs**: View historical job execution

### 2. Customize Fraud Detection Rules

Edit `flink/jobs/FraudDetectionJob.java`:
- Adjust thresholds (currently $10k for high-value)
- Add new fraud patterns
- Modify CEP patterns
- Change alert severities

Then rebuild and resubmit:
```bash
cd flink && ./build-and-submit.sh fraud-detection
```

### 3. Query Results in Trino

Once data flows through, query in Trino:

```sql
-- Connect to Trino
docker exec -it trino trino

-- Query fraud alerts (if written to Iceberg)
SELECT * FROM iceberg.default.fraud_alerts
WHERE detected_at > current_timestamp - INTERVAL '1' HOUR;
```

### 4. Set Up Monitoring

Add Flink metrics to Grafana:
1. Open Grafana: http://localhost:3000
2. Add Prometheus data source: http://prometheus:9090
3. Import Flink dashboard
4. Monitor job throughput, latency, state size

### 5. Scale the Cluster

Add more TaskManagers for higher throughput:

Edit `docker-compose.yml` and add:
```yaml
flink-taskmanager-3:
  # Copy configuration from flink-taskmanager-1
  # Change ports to avoid conflicts
```

Then:
```bash
docker-compose up -d flink-taskmanager-3
```

## Useful Commands

```bash
# List all running jobs
docker exec flink-jobmanager /opt/flink/bin/flink list -r

# List all jobs (including finished)
docker exec flink-jobmanager /opt/flink/bin/flink list -a

# Cancel a job
docker exec flink-jobmanager /opt/flink/bin/flink cancel <JOB_ID>

# Create savepoint
docker exec flink-jobmanager /opt/flink/bin/flink savepoint <JOB_ID>

# View Flink logs
docker-compose logs -f flink-jobmanager
docker-compose logs -f flink-taskmanager-1

# Restart Flink cluster
docker-compose restart flink-jobmanager flink-taskmanager-1 flink-taskmanager-2

# Stop Flink cluster
docker-compose stop flink-jobmanager flink-taskmanager-1 flink-taskmanager-2
```

## Common Configuration Changes

### Increase Checkpoint Frequency

Edit `flink/conf/flink-conf.yaml`:
```yaml
execution.checkpointing.interval: 10000  # 10 seconds instead of 30
```

### Adjust Memory

Edit `flink/conf/flink-conf.yaml`:
```yaml
taskmanager.memory.process.size: 8192m  # Increase to 8GB
```

### Change Parallelism

Edit `flink/conf/flink-conf.yaml`:
```yaml
parallelism.default: 4  # Increase to 4 parallel tasks
```

Then restart Flink cluster:
```bash
docker-compose restart flink-jobmanager flink-taskmanager-1 flink-taskmanager-2
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Flink Cluster                            │
│                                                             │
│  ┌──────────────┐      ┌──────────────┐  ┌──────────────┐ │
│  │ JobManager   │──────│ TaskManager-1│  │ TaskManager-2│ │
│  │ (Coordinator)│      │ (4 slots)    │  │ (4 slots)    │ │
│  └──────────────┘      └──────────────┘  └──────────────┘ │
│                                                             │
│  ┌────────────────────────────────────────────────────────┐│
│  │              Running Jobs                              ││
│  │  • Fraud Detection (parallelism=2)                     ││
│  │  • Transaction Monitoring (parallelism=2)              ││
│  │  • Balance Aggregation (parallelism=2)                 ││
│  └────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                        ▲                │
                        │                ▼
          ┌─────────────────────────────────────┐
          │         Kafka Topics                │
          │  IN:  banking.transactions.raw      │
          │  OUT: banking.fraud.alerts          │
          │       banking.balances.realtime     │
          │       banking.monitoring.alerts     │
          └─────────────────────────────────────┘
```

## Learn More

- [Full Flink Integration Guide](FLINK_INTEGRATION.md)
- [Flink Official Documentation](https://flink.apache.org/docs/stable/)
- [Banking Use Cases](FLINK_INTEGRATION.md#flink-jobs-for-banking)
- [Performance Tuning](FLINK_INTEGRATION.md#performance-tuning)

## Support

If you encounter issues:
1. Check logs: `docker-compose logs flink-jobmanager flink-taskmanager-1`
2. Verify services: `docker-compose ps`
3. Review [Troubleshooting section](FLINK_INTEGRATION.md#troubleshooting)
4. Check Flink UI: http://localhost:8081

Happy streaming! 🚀
