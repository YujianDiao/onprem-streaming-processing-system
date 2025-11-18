# Kafka Dashboard Import Guide

## Issue: Pre-built Kafka Dashboards Are Empty

**Problem:** Dashboard IDs 7589 and 11962 are designed for `kafka_exporter`, not JMX Exporter.

**Solution:** Use the custom Kafka JMX dashboard created specifically for your setup.

---

## Import the Custom Kafka JMX Dashboard

### Method 1: Import via Grafana UI (Recommended)

1. **Open the dashboard JSON file:**
   ```bash
   cat /tmp/kafka-jmx-dashboard.json
   ```

2. **Copy the entire JSON content**

3. **In Grafana:**
   - Click **"+"** (Create) → **"Import"**
   - Click **"Import via panel json"**
   - Paste the JSON content
   - Click **"Load"**
   - Select **"Prometheus"** as datasource
   - Click **"Import"**

### Method 2: Save to Grafana Provisioning (Auto-load on restart)

```bash
# Copy dashboard to Grafana dashboards directory
sudo cp /tmp/kafka-jmx-dashboard.json /home/diao/workspace/onprem-streaming-processing-system/monitoring/grafana/dashboards/

# Restart Grafana to auto-load
docker compose restart grafana
```

---

## What the Dashboard Shows

The custom Kafka JMX dashboard includes:

### 📊 Panel 1: Kafka Broker Throughput (Bytes/sec)
- Shows bytes in/out per second for each broker
- **Metrics:**
  - `kafka_server_brokertopicmetrics_bytesinpersec_count`
  - `kafka_server_brokertopicmetrics_bytesoutpersec_count`

### 📊 Panel 2: Partition Leaders per Broker
- Shows how partition leaders are distributed
- **Metric:** `kafka_server_replicamanager_leadercount`
- **Current:** kafka-1: 21, kafka-2: 21, kafka-3: 22 (balanced)

### 📊 Panel 3: Kafka Messages In per Second
- Shows message ingestion rate per broker
- **Metric:** `kafka_server_brokertopicmetrics_messagesinpersec_count`

### 📊 Panel 4: Under-Replicated Partitions
- Critical health metric (should always be 0)
- **Metric:** `kafka_server_replicamanager_underreplicatedpartitions`
- **Alert:** Turns red if > 0

### 📊 Panel 5: Request Handler Idle Percent
- Shows broker capacity (higher is better)
- **Metric:** `kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_oneminuterate`

### 📊 Panel 6: Total Partitions per Broker
- Shows partition distribution
- **Metric:** `kafka_server_replicamanager_partitioncount`

---

## Test Queries in Grafana Explore

Before importing the dashboard, test these queries in Explore:

### Query 1: Broker Throughput
```promql
rate(kafka_server_brokertopicmetrics_bytesinpersec_count{broker_host=~"kafka-.*"}[5m])
```

### Query 2: Leader Count
```promql
kafka_server_replicamanager_leadercount{broker_host=~"kafka-.*"}
```

### Query 3: Messages In Rate
```promql
rate(kafka_server_brokertopicmetrics_messagesinpersec_count{broker_host=~"kafka-.*"}[5m])
```

### Query 4: Under-Replicated Partitions
```promql
kafka_server_replicamanager_underreplicatedpartitions{broker_host=~"kafka-.*"}
```

---

## Available Kafka Metrics

Your JMX exporters provide these metric categories:

### Broker Topic Metrics
- `kafka_server_brokertopicmetrics_bytesinpersec_*`
- `kafka_server_brokertopicmetrics_bytesoutpersec_*`
- `kafka_server_brokertopicmetrics_messagesinpersec_*`
- `kafka_server_brokertopicmetrics_totalfetchrequestspersec_*`
- `kafka_server_brokertopicmetrics_totalproducerequestspersec_*`

### Replica Manager Metrics
- `kafka_server_replicamanager_leadercount`
- `kafka_server_replicamanager_partitioncount`
- `kafka_server_replicamanager_underreplicatedpartitions`
- `kafka_server_replicamanager_isrexpandspersec_*`
- `kafka_server_replicamanager_isrshrinkspersec_*`

### Request Handler Metrics
- `kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_*`

### Network Metrics
- `kafka_network_requestmetrics_totaltimems_*`
- `kafka_network_processor_idlepercent_*`

### Log Metrics
- `kafka_log_log_logendoffset`
- `kafka_log_log_size`

---

## Creating Custom Panels

To add more panels to your dashboard:

1. **Click "Add panel"** in the dashboard
2. **Select metric** from the metric browser
3. **Use these label filters:**
   - `broker_host=~"kafka-.*"` - All brokers
   - `broker_host="kafka-1"` - Specific broker
   - `topic="banking-transactions"` - Specific topic

### Example: Topic-Specific Throughput
```promql
rate(kafka_server_brokertopicmetrics_bytesinpersec_topic_banking_transactions_count[5m])
```

### Example: Per-Topic Messages
```promql
rate(kafka_server_brokertopicmetrics_messagesinpersec_topic_banking_transactions_count[5m])
```

---

## Alternative: Dashboard 19004 (Kafka JMX)

There's another community dashboard designed for JMX metrics:

**Dashboard ID: 19004** - Kafka Overview (JMX)

Try importing this as well:
1. Grafana → + → Import
2. Enter: **19004**
3. Select Prometheus datasource
4. Click Import

This dashboard may need minor adjustments for metric names.

---

## Why Other Dashboards Don't Work

**Dashboard 7589 & 11962:**
- Designed for `danielqsj/kafka_exporter`
- Expects metrics like: `kafka_brokers`, `kafka_topic_partitions`
- **Your setup uses:** JMX Exporter (different metric names)

**Your JMX Exporter provides:**
- `kafka_server_*` metrics from Kafka MBeans
- More detailed metrics but different naming

---

## Verify Metrics Are Available

### Check in Prometheus
```bash
# All Kafka metrics
curl -s http://localhost:9090/api/v1/label/__name__/values | jq '.data[]' | grep kafka_server | wc -l

# Expected: 100+ metrics
```

### Check JMX Exporter Directly
```bash
# Broker 1 metrics
curl -s http://localhost:10001/metrics | grep kafka_server | head -20

# Broker 2 metrics
curl -s http://localhost:10002/metrics | grep kafka_server | head -20

# Broker 3 metrics
curl -s http://localhost:10003/metrics | grep kafka_server | head -20
```

---

## Dashboard Features

The custom dashboard provides:

✅ **Auto-refresh:** Every 30 seconds
✅ **Time range:** Last 15 minutes (adjustable)
✅ **Multi-broker view:** All 3 brokers on same charts
✅ **Health indicators:** Color-coded gauges
✅ **Trend analysis:** Line graphs with mean/current values

---

## Next Steps

1. **Import the custom Kafka JMX dashboard** using Method 1 above
2. **Verify you see data** in all panels
3. **Customize as needed:**
   - Add more panels for specific topics
   - Adjust time ranges
   - Add alerting thresholds
4. **Save your changes**

---

## Troubleshooting

### Dashboard shows "No Data"

**Check time range:**
- Top right corner, try "Last 5 minutes"
- Make sure it's not set to a future time

**Verify Prometheus has data:**
```bash
curl -s 'http://localhost:9090/api/v1/query?query=kafka_server_replicamanager_leadercount' | jq
```

**Check JMX exporters are UP:**
```bash
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | select(.labels.job | contains("kafka"))'
```

### Metrics show but graphs are flat

- Data generator may not be producing enough load
- Try a shorter time range (Last 5 minutes)
- Check if Kafka is receiving messages:
  ```bash
  docker logs data-generator --tail 20
  ```

---

## Summary

**Issue:** Pre-built Kafka dashboards (7589, 11962) expect different exporter
**Solution:** Use custom Kafka JMX dashboard at `/tmp/kafka-jmx-dashboard.json`
**Import:** Copy JSON → Grafana → + → Import → Paste JSON
**Result:** Working Kafka dashboard with 6 key metrics panels

The custom dashboard is specifically designed for your JMX exporter setup and will show real-time Kafka cluster metrics immediately!
