# Monitoring Quick Start - Get Grafana Working NOW

**Goal:** See your first monitoring dashboard with real data in 5 minutes!

---

## Current Status

✅ Prometheus is running and collecting metrics
✅ Grafana is running and accessible
✅ 2 services being monitored (Prometheus, Grafana)

---

## Step-by-Step Guide

### Step 1: Access Grafana (1 minute)

```bash
# Open Grafana in your browser
http://localhost:3000
```

**Login:**
- Username: `admin`
- Password: `admin`

When prompted, change the password (or click "Skip").

---

### Step 2: Verify Prometheus Datasource (1 minute)

1. In Grafana, click **Configuration** (gear icon) → **Data Sources**
2. Click **"Add data source"**
3. Select **"Prometheus"**
4. Configure:
   - **Name:** `Prometheus`
   - **URL:** `http://prometheus:9090`
   - Leave other settings as default
5. Click **"Save & Test"** at the bottom
6. You should see: ✅ **"Data source is working"**

---

### Step 3: Import Prometheus Dashboard (2 minutes)

1. Click **"+"** (Create) → **"Import"**
2. **Dashboard ID:** Enter `3662`
3. Click **"Load"**
4. **Select Prometheus datasource:** Choose "Prometheus"
5. Click **"Import"**

**🎉 You now have a working dashboard with real metrics!**

---

### Step 4: Explore Your Data (1 minute)

#### In the Dashboard You Just Imported:
- Look at **"Prometheus Stats"** panels
- See memory usage, scrape duration, query performance
- All showing real-time data from your Prometheus instance

#### Try the Explore Tab:
1. Click **"Explore"** (compass icon) in left sidebar
2. Make sure **"Prometheus"** is selected as datasource
3. Try these queries:

**Query 1: Service Availability**
```promql
up
```
Click **"Run query"** - You'll see which services are UP (value: 1) or DOWN (value: 0)

**Query 2: Prometheus Memory Usage (in MB)**
```promql
process_resident_memory_bytes{job="prometheus"} / 1024 / 1024
```

**Query 3: Request Rate**
```promql
rate(prometheus_http_requests_total[5m])
```

---

## What's Working vs What's Not

### ✅ Currently Monitored (Working)
- **Prometheus itself** - Memory, CPU, scrape stats, queries
- **Grafana** - Internal metrics, sessions, requests

### ❌ Not Yet Monitored (Need Additional Setup)
- **Kafka** - Requires JMX exporter
- **PostgreSQL** - Requires postgres_exporter
- **Spark** - Requires metrics sink configuration
- **MinIO** - Requires bearer token authentication
- **Container metrics** - Requires cAdvisor

**To add these:** See `MONITORING_ENHANCEMENTS.md`

---

## Quick Tests

### Test 1: Check Prometheus is Scraping
```bash
# See what targets Prometheus is monitoring
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'
```

**Expected Output:**
```json
{
  "job": "prometheus",
  "health": "up"
}
{
  "job": "grafana",
  "health": "up"
}
```

### Test 2: Query Metrics from CLI
```bash
# Check service availability
curl -s 'http://localhost:9090/api/v1/query?query=up' | jq '.data.result[] | {job: .metric.job, value: .value[1]}'
```

**Expected Output:**
```json
{
  "job": "prometheus",
  "value": "1"
}
{
  "job": "grafana",
  "value": "1"
}
```

---

## More Dashboards to Import

Once you're comfortable, try importing these dashboards:

### Prometheus Monitoring Dashboards
- **Dashboard ID: 3662** - Prometheus 2.0 Overview (✅ you just imported this!)
- **Dashboard ID: 15489** - Prometheus Stats (alternative)
- **Dashboard ID: 12633** - Prometheus Metrics (simpler)

### After Adding cAdvisor (Container Metrics)
- **Dashboard ID: 193** - Docker & System Monitoring
- **Dashboard ID: 14282** - cAdvisor Prometheus

### After Adding Node Exporter (Host Metrics)
- **Dashboard ID: 1860** - Node Exporter Full

### After Adding Kafka JMX Exporter
- **Dashboard ID: 7589** - Kafka Exporter Overview
- **Dashboard ID: 11962** - Kafka Cluster Monitoring

### After Adding PostgreSQL Exporter
- **Dashboard ID: 9628** - PostgreSQL Database
- **Dashboard ID: 6742** - PostgreSQL Overview

---

## Understanding Your Dashboard

### What the Prometheus Dashboard Shows:

1. **Prometheus Stats**
   - How much memory Prometheus is using
   - How many time series are being tracked
   - Storage usage

2. **Scrape Information**
   - How long it takes to scrape metrics
   - Success/failure rates
   - Number of samples collected

3. **Query Performance**
   - How fast queries execute
   - Query rates
   - Concurrent queries

4. **Resource Usage**
   - CPU usage
   - Memory allocation
   - Garbage collection stats

---

## Creating a Custom Dashboard

Want to create your own dashboard?

1. Click **"+"** → **"Dashboard"**
2. Click **"Add new panel"**
3. **Enter a query:** `up`
4. **Choose visualization:** Stat, Graph, Gauge, etc.
5. **Set title:** "Service Availability"
6. Click **"Apply"**
7. Click **Save** icon (disk, top right)
8. Name your dashboard: "My Platform Monitor"

---

## Common PromQL Queries for Your Platform

```promql
# Services that are UP (1) or DOWN (0)
up

# Prometheus memory usage in MB
process_resident_memory_bytes{job="prometheus"} / 1024 / 1024

# Number of metrics Prometheus is collecting
prometheus_tsdb_symbol_table_size_bytes

# Prometheus scrape success rate
rate(prometheus_target_scrapes_sample_scraped_total[5m])

# Prometheus HTTP request rate
rate(prometheus_http_requests_total[5m])

# Prometheus query duration (p99)
histogram_quantile(0.99, rate(prometheus_engine_query_duration_seconds_bucket[5m]))

# Grafana active users/sessions
grafana_stat_active_users

# Grafana request rate
rate(grafana_http_request_duration_seconds_count[5m])
```

---

## Next Steps

### Option 1: Just Use What's Working (Easiest)
- ✅ You now have basic monitoring of Prometheus and Grafana
- ✅ You can see metrics, create dashboards, set up alerts
- ⏭️ This is enough for monitoring the monitoring system itself

### Option 2: Add Container Monitoring (15 minutes)
- Follow **MONITORING_ENHANCEMENTS.md** → **Phase 1**
- Add **cAdvisor** and **Node Exporter**
- Get CPU, memory, network metrics for ALL 13 containers
- Import Docker monitoring dashboards

### Option 3: Add Full Platform Monitoring (2-3 hours)
- Follow **MONITORING_ENHANCEMENTS.md** → All Phases
- Add Kafka JMX exporter, PostgreSQL exporter, Spark metrics
- Get complete visibility into data pipeline health

---

## Troubleshooting

### "No data" in Dashboard
- **Check time range:** Top right corner, try "Last 5 minutes"
- **Verify datasource:** Configuration → Data Sources → Test
- **Try Explore:** Use simple query `up` to see if any data exists

### Dashboard Import Failed
- Make sure you selected Prometheus datasource during import
- Try a different dashboard ID if one doesn't work
- Check Grafana logs: `docker compose logs grafana --tail 50`

### Prometheus Datasource Test Failed
- Make sure Prometheus is running: `docker compose ps prometheus`
- URL should be: `http://prometheus:9090` (NOT localhost)
- Check Grafana can reach Prometheus: `docker exec grafana wget -O- http://prometheus:9090/api/v1/query?query=up`

---

## Success Checklist

✅ I can login to Grafana at http://localhost:3000
✅ Prometheus datasource is configured and tested
✅ I imported the Prometheus dashboard (ID 3662)
✅ I can see metrics and graphs with real data
✅ I tried the Explore tab with query: `up`
✅ I understand what's being monitored vs what needs exporters

**Status:** 🎉 **YOU HAVE WORKING MONITORING!**

While it's basic (only monitoring Prometheus and Grafana), you now have:
- A functioning monitoring stack
- Real metrics being collected
- Visual dashboards showing data
- A foundation to build on

To add more metrics (Kafka, PostgreSQL, Spark, containers), see:
- **MONITORING_ENHANCEMENTS.md** - Step-by-step guide for each exporter
- **MONITORING_WORKING_STATUS.md** - Current status and configuration details

---

**Happy Monitoring! 📊**
