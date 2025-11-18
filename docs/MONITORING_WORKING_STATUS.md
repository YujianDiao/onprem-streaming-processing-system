# Monitoring Status - Prometheus & Grafana

**Last Updated:** 2025-11-16
**Status:** ✅ **BASIC MONITORING OPERATIONAL**

---

## Quick Summary

✅ **Working Now:**
- Prometheus: Collecting metrics (2/2 targets UP)
- Grafana: Web UI accessible at http://localhost:3000
- Basic metrics available for Prometheus and Grafana themselves

⚠️ **Requires Additional Configuration:**
- Kafka metrics (needs JMX exporter)
- PostgreSQL metrics (needs postgres_exporter)
- Spark metrics (needs metrics sink configuration)
- MinIO metrics (needs bearer token authentication)

---

## Current Working Metrics

### ✅ Prometheus Self-Monitoring (UP)
- **Target:** `localhost:9090`
- **Status:** ✅ UP
- **Metrics Available:**
  - Prometheus memory usage
  - Scrape duration and success rate
  - Active time series count
  - Query performance
  - Storage usage

### ✅ Grafana Monitoring (UP)
- **Target:** `grafana:3000`
- **Status:** ✅ UP
- **Metrics Available:**
  - Grafana internal metrics
  - Request rates
  - Response times
  - Active sessions

### ❌ MinIO Monitoring (DISABLED)
- **Target:** `minio:9000`
- **Status:** ❌ Disabled
- **Issue:** Requires AWS4-HMAC-SHA256 authentication or bearer token
- **Solution:** See "Enabling MinIO Metrics" section below

---

## Access Information

### Prometheus
- **URL:** http://localhost:9090
- **Credentials:** None required
- **What to Check:**
  1. Go to Status → Targets
  2. Should see 2/2 targets UP (prometheus, grafana)
  3. Go to Graph tab, try query: `up`

### Grafana
- **URL:** http://localhost:3000
- **Default Login:**
  - Username: `admin`
  - Password: `admin`
- **What to Do:**
  1. Login and change password when prompted
  2. Verify Prometheus datasource (Configuration → Data Sources)
  3. Use Explore to test queries

---

## Testing Your Monitoring Setup

### Test 1: Check Prometheus Targets
```bash
# From terminal
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'

# Expected output:
# {
#   "job": "prometheus",
#   "health": "up"
# }
# {
#   "job": "grafana",
#   "health": "up"
# }
```

### Test 2: Query Metrics from CLI
```bash
# Check which services are being monitored
curl -s 'http://localhost:9090/api/v1/query?query=up' | jq '.data.result[] | {job: .metric.job, status: .value[1]}'

# Check Prometheus memory usage
curl -s 'http://localhost:9090/api/v1/query?query=process_resident_memory_bytes{job="prometheus"}' | jq '.data.result[0].value[1]'
```

### Test 3: Use Grafana Explore
1. Open Grafana: http://localhost:3000
2. Login (admin/admin)
3. Click "Explore" icon (compass) in left sidebar
4. Select "Prometheus" as datasource
5. Try these queries:
   - `up` - Shows service availability
   - `process_resident_memory_bytes` - Shows memory usage
   - `rate(prometheus_http_requests_total[5m])` - Request rate

---

## Setting Up Grafana Datasource (Manual)

If Prometheus datasource is not automatically configured:

1. **Go to Grafana:** http://localhost:3000
2. **Navigate:** Configuration → Data Sources
3. **Click:** "Add data source"
4. **Select:** Prometheus
5. **Configure:**
   - Name: `Prometheus`
   - URL: `http://prometheus:9090`
   - Access: `Server (default)`
   - Scrape interval: `15s`
6. **Click:** "Save & Test"
7. **Verify:** Should see "Data source is working"

---

## Setting Up Grafana Datasource (Automated)

To automatically configure Prometheus datasource on Grafana startup:

```bash
# 1. Copy the datasource configuration file
cat > /tmp/datasource.yml <<'EOF'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
    jsonData:
      timeInterval: 15s
      queryTimeout: 60s
      httpMethod: POST
    version: 1
EOF

# 2. Copy file to Grafana provisioning directory (requires Docker to be stopped or manual copy)
# Note: The grafana provisioning directory is owned by root, so you may need to:
# - Stop Grafana: docker compose stop grafana
# - Copy with sudo or as root
# - Start Grafana: docker compose start grafana

# Alternative: Mount the file in docker-compose.yml
# Add to grafana service volumes:
#   - /tmp/datasource.yml:/etc/grafana/provisioning/datasources/prometheus.yml:ro
```

---

## Creating Your First Dashboard

### Method 1: Manual Creation in Grafana UI

1. Login to Grafana: http://localhost:3000
2. Click "+" → "Dashboard" → "Add new panel"
3. Configure panel:
   - **Query:** `up`
   - **Legend:** `{{job}}`
   - **Panel Title:** "Service Availability"
4. Click "Apply"
5. Click "Save dashboard" icon (top right)
6. Name it: "Data Platform Status"

### Method 2: Import Pre-Built Dashboard

**For Prometheus Monitoring:**
1. In Grafana, click "+" → "Import"
2. Enter Dashboard ID: **3662** (Prometheus 2.0 Overview)
3. Select Prometheus datasource
4. Click "Import"

**Other Useful Dashboards:**
- **15489** - Prometheus Stats (alternative)
- **13659** - Prometheus Monitoring
- **12633** - Prometheus Metrics (simple)

---

## Sample Queries to Try

### Service Availability
```promql
up
```
Shows 1 for UP, 0 for DOWN for each monitored service.

### Prometheus Memory Usage
```promql
process_resident_memory_bytes{job="prometheus"} / 1024 / 1024
```
Shows Prometheus memory in MB.

### Scrape Success Rate
```promql
rate(prometheus_target_scrapes_sample_scraped_total[5m])
```
Shows how many metrics samples are being collected per second.

### HTTP Request Rate
```promql
rate(prometheus_http_requests_total[5m])
```
Shows Prometheus HTTP request rate over 5 minutes.

### Time Series Count
```promql
prometheus_tsdb_symbol_table_size_bytes
```
Shows total unique time series in Prometheus.

---

## Enabling MinIO Metrics (Optional)

MinIO requires AWS Signature v4 authentication for metrics endpoint. To enable:

### Option 1: Generate Bearer Token (Recommended)

```bash
# 1. Install mc (MinIO client) if not available
# Already available via minio-init container

# 2. Generate Prometheus bearer token
docker exec minio-init mc admin prometheus generate myminio

# Expected output will include:
# bearer_token: YOUR_BEARER_TOKEN

# 3. Update monitoring/prometheus/prometheus.yml
# Uncomment the MinIO job and add the bearer token:

# - job_name: 'minio'
#   metrics_path: /minio/v2/metrics/cluster
#   scheme: http
#   bearer_token: 'YOUR_BEARER_TOKEN_HERE'
#   static_configs:
#     - targets: ['minio:9000']
#       labels:
#         service: 'minio'

# 4. Restart Prometheus
docker compose restart prometheus
```

### Option 2: Enable Public Metrics (Less Secure)

Add to MinIO environment in `docker-compose.yml`:

```yaml
  minio:
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
      MINIO_DOMAIN: minio
      MINIO_PROMETHEUS_AUTH_TYPE: public  # Add this line
```

Then restart MinIO and Prometheus:
```bash
docker compose restart minio prometheus
```

---

## Next Steps for Full Monitoring

To get comprehensive platform monitoring, see **MONITORING_ENHANCEMENTS.md** for:

### Quick Win (15 minutes)
1. **Add cAdvisor** - Get CPU, memory, network metrics for ALL containers
2. **Add Node Exporter** - Get host system metrics

### Critical Data Pipeline (1-2 hours)
3. **Add Kafka JMX Exporter** - Monitor message flow and consumer lag
4. **Add PostgreSQL Exporter** - Monitor metastore database

### Processing Metrics (2-3 hours)
5. **Configure Spark Metrics Sink** - Monitor stream processing jobs

---

## Troubleshooting

### Prometheus Targets Are Down

```bash
# Check Prometheus logs
docker compose logs prometheus --tail 50

# Check if Prometheus can reach Grafana
docker exec prometheus wget -O- http://grafana:3000/metrics 2>&1 | head

# Verify Docker network
docker network inspect data-platform | grep -E "prometheus|grafana"
```

### Grafana Can't Connect to Prometheus

```bash
# Test from Grafana container
docker exec grafana wget -O- http://prometheus:9090/api/v1/query?query=up

# Check Grafana logs
docker compose logs grafana --tail 50
```

### No Metrics Showing in Dashboard

1. **Check Datasource:**
   - Go to Configuration → Data Sources
   - Click on Prometheus
   - Scroll down and click "Test"
   - Should show "Data source is working"

2. **Check Time Range:**
   - Grafana defaults to "Last 6 hours"
   - Prometheus may have just started
   - Change to "Last 5 minutes"

3. **Try Simple Query:**
   - Go to Explore
   - Enter: `up`
   - Should show results for prometheus and grafana

### Grafana Not Saving Dashboard

- Login with admin user (not viewer)
- Check browser console for errors
- Verify Grafana has write permissions to its data directory

---

## Summary

### ✅ What's Working Right Now
- Prometheus collecting basic metrics (2 targets UP)
- Grafana web interface accessible
- Can query Prometheus and Grafana's own metrics
- Foundation ready for adding more exporters

### 📋 Configuration Files Created
1. ✅ `monitoring/prometheus/prometheus.yml` - Updated with working targets only
2. ✅ `monitoring/prometheus/prometheus.yml.backup` - Original config backup
3. ⏸️ `/tmp/grafana-datasource-prometheus.yml` - Grafana datasource config (optional)
4. ⏸️ `/tmp/grafana-dashboard-provisioning.yml` - Dashboard provisioning (optional)
5. ⏸️ `/tmp/grafana-platform-status-dashboard.json` - Sample dashboard (optional)

### 🎯 Current Monitoring Coverage
- **Prometheus itself:** ✅ 100%
- **Grafana:** ✅ 100%
- **MinIO:** ⏸️ Disabled (can be enabled with bearer token)
- **Kafka:** ❌ Not monitored (needs JMX exporter)
- **PostgreSQL:** ❌ Not monitored (needs postgres_exporter)
- **Spark:** ❌ Not monitored (needs metrics configuration)

### 📈 Next Actions

**To get monitoring working with data:**
1. ✅ Prometheus and Grafana are running
2. ✅ Basic metrics are available
3. ⏭️ Login to Grafana and explore metrics with queries shown above
4. ⏭️ Import Prometheus dashboard (ID 3662) to visualize data
5. ⏭️ (Optional) Enable MinIO metrics with bearer token
6. ⏭️ (Optional) Add more exporters per MONITORING_ENHANCEMENTS.md

---

**Status:** 🟢 **BASIC MONITORING FUNCTIONAL**

You now have a working Prometheus + Grafana setup. While it doesn't monitor all platform services yet, it's operational and ready to visualize the metrics that are available. Import the Prometheus dashboard (ID 3662) in Grafana to see your first monitoring dashboard with real data!
