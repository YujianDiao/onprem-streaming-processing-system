# Import Kafka Dashboard - Quick Steps

## The Problem
Dashboard IDs 7589 and 11962 won't work because they're designed for a different Kafka exporter. Your setup uses JMX Exporter which has different metric names.

## The Solution
I've created a custom Kafka dashboard specifically for your JMX metrics.

---

## STEP-BY-STEP: Import the Dashboard

### Step 1: Copy the Dashboard JSON

```bash
cat /tmp/kafka-jmx-dashboard.json
```

**Action:** Copy ALL the output (the entire JSON)

---

### Step 2: Open Grafana

1. Go to: **http://localhost:3000**
2. Login: **admin** / **admin**

---

### Step 3: Import the Dashboard

1. Click the **"+"** icon (left sidebar)
2. Click **"Import"**
3. Click **"Import via panel json"** (right side)
4. **Paste** the JSON you copied in Step 1
5. Click **"Load"**
6. Under **"Prometheus"** dropdown, select: **Prometheus**
7. Click **"Import"**

---

## What You'll See

The dashboard will immediately show:

✅ **Broker Throughput** - Bytes in/out per second
✅ **Partition Leaders** - Distribution across brokers (21, 21, 22)
✅ **Messages Rate** - Messages per second
✅ **Under-Replicated Partitions** - Health indicator (should be 0)
✅ **Request Handler Idle %** - Broker capacity
✅ **Total Partitions** - Per broker

---

## Verify Before Importing

Test that metrics are available:

```bash
# Check leader distribution
curl -s 'http://localhost:9090/api/v1/query?query=kafka_server_replicamanager_leadercount' | jq '.data.result[] | {broker: .metric.broker_host, leaders: .value[1]}'
```

**Expected output:**
```json
{
  "broker": "kafka-1",
  "leaders": "21"
}
{
  "broker": "kafka-2",
  "leaders": "21"
}
{
  "broker": "kafka-3",
  "leaders": "22"
}
```

✅ If you see this, the dashboard will work!

---

## Alternative: Try Dashboard 19004

If you want to try a community dashboard designed for JMX metrics:

1. In Grafana → **+** → **Import**
2. Enter ID: **19004**
3. Click **Load**
4. Select **Prometheus** datasource
5. Click **Import**

This may need some adjustments but could work.

---

## Troubleshooting

### "No Data" in dashboard panels

**Fix 1: Check time range**
- Top right corner
- Change to **"Last 5 minutes"**

**Fix 2: Verify Prometheus datasource**
- Configuration → Data Sources
- Click "Prometheus"
- Click "Save & Test"
- Should say "Data source is working"

**Fix 3: Check Kafka exporters are UP**
```bash
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | select(.labels.job | contains("kafka")) | {job: .labels.job, health: .health}'
```

All should show `"health": "up"`

---

## Summary

1. ✅ Copy JSON from `/tmp/kafka-jmx-dashboard.json`
2. ✅ Grafana → + → Import → Paste JSON
3. ✅ Select Prometheus datasource
4. ✅ Click Import
5. ✅ See your Kafka metrics!

**Dashboard name:** "Kafka JMX Metrics - Cluster Overview"

The dashboard auto-refreshes every 30 seconds and shows the last 15 minutes of data.

---

**Need help?** Check `KAFKA_DASHBOARD_IMPORT_GUIDE.md` for detailed information.
