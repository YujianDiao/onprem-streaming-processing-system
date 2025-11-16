# Monitoring Enhancements - Adding Full Metrics Coverage

**Current Status:** ✅ Basic monitoring working (Prometheus, Grafana, MinIO)
**Goal:** Add comprehensive metrics for all platform services

---

## Current Monitoring Status

### ✅ Working Metrics (Out of the Box)

| Service | Status | Endpoint | Metrics Available |
|---------|--------|----------|-------------------|
| Prometheus | ✅ UP | localhost:9090 | Self-monitoring, scrape stats |
| Grafana | ✅ UP | grafana:3000 | Internal Grafana metrics |
| MinIO | ✅ UP | minio:9000/minio/v2/metrics/cluster | Storage, bandwidth, requests |

### ❌ Services Requiring Exporters

| Service | Current Status | Requires | Priority |
|---------|---------------|----------|----------|
| Kafka (3 brokers) | ❌ DOWN | JMX Exporter | HIGH |
| PostgreSQL | ❌ DOWN | postgres_exporter | MEDIUM |
| Spark Master | ❌ DOWN | Metrics Sink Config | MEDIUM |
| Spark Workers (2) | ❌ DOWN | Metrics Sink Config | MEDIUM |
| Hive Metastore | ❌ No metrics | JMX Exporter | LOW |
| Trino | ❌ No metrics | JMX/HTTP Exporter | LOW |

---

## Why Services Don't Have Metrics

### Kafka Brokers
**Issue:** Kafka exposes JMX metrics, but they're not in Prometheus format
**Solution:** Add JMX Exporter as sidecar containers or configure Kafka to use Prometheus JMX agent

**What Metrics Would Provide:**
- Broker health and uptime
- Topic throughput (messages/sec, bytes/sec)
- Consumer lag (critical for stream processing)
- Partition replication status
- Network I/O and request rates
- Disk usage per topic

### PostgreSQL (Hive Metastore Backend)
**Issue:** PostgreSQL doesn't expose Prometheus-compatible metrics
**Solution:** Add `postgres_exporter` service

**What Metrics Would Provide:**
- Database connections and queries
- Table sizes and row counts
- Cache hit ratios
- Query performance
- Replication status (if configured)

### Spark Cluster
**Issue:** Spark UI exposes metrics as HTML, not Prometheus format
**Solution:** Configure Spark Metrics Sink with Prometheus support

**What Metrics Would Provide:**
- Active/completed jobs
- Stage execution times
- Executor memory usage
- Shuffle read/write
- Task success/failure rates
- GC time

---

## Enhancement Options

### Option 1: Quick Win - Container Metrics (Recommended First)

Add **cAdvisor** to monitor ALL containers' resource usage.

**Benefit:** Get CPU, memory, network, disk metrics for all services with ONE addition.

#### Add to docker-compose.yml:

```yaml
  cadvisor:
    image: gcr.io/cadvisor/cadvisor:latest
    hostname: cadvisor
    container_name: cadvisor
    ports:
      - "8085:8080"
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
    networks:
      - data-platform
    restart: unless-stopped
    privileged: true
    devices:
      - /dev/kmsg
```

#### Update monitoring/prometheus/prometheus.yml:

```yaml
  - job_name: 'cadvisor'
    static_configs:
      - targets: ['cadvisor:8080']
        labels:
          service: 'container-metrics'
```

**Then restart:**
```bash
docker compose up -d cadvisor
docker compose restart prometheus
```

**Result:** You'll have CPU, memory, network metrics for ALL 13 containers!

---

### Option 2: Kafka Metrics (HIGH Priority)

Kafka metrics are critical for understanding data pipeline health.

#### Method A: JMX Exporter Sidecar (Easier)

Add JMX exporter as separate services:

```yaml
  kafka-1-jmx-exporter:
    image: bitnami/jmx-exporter:latest
    hostname: kafka-1-jmx-exporter
    container_name: kafka-1-jmx-exporter
    ports:
      - "9101:9101"
    environment:
      - SERVICE_PORT=9101
      - JMX_HOST=kafka-1
      - JMX_PORT=9999
    networks:
      - data-platform
    depends_on:
      - kafka-1
    restart: unless-stopped

  # Repeat for kafka-2 (port 9102) and kafka-3 (port 9103)
```

#### Method B: Prometheus JMX Agent in Kafka (More Integrated)

1. Download Prometheus JMX exporter JAR
2. Add to Kafka containers as Java agent
3. Modify Kafka environment variables

**Detailed setup in KAFKA_JMX_SETUP.md (to be created if needed)**

#### Update Prometheus config:

```yaml
  - job_name: 'kafka-broker-1'
    static_configs:
      - targets: ['kafka-1:9101']  # or kafka-1-jmx-exporter:9101
        labels:
          service: 'kafka'
          broker_id: '1'

  - job_name: 'kafka-broker-2'
    static_configs:
      - targets: ['kafka-2:9102']
        labels:
          service: 'kafka'
          broker_id: '2'

  - job_name: 'kafka-broker-3'
    static_configs:
      - targets: ['kafka-3:9103']
        labels:
          service: 'kafka'
          broker_id: '3'
```

---

### Option 3: PostgreSQL Metrics

Add postgres_exporter to monitor the Hive Metastore database.

#### Add to docker-compose.yml:

```yaml
  postgres-exporter:
    image: prometheuscommunity/postgres-exporter:latest
    hostname: postgres-exporter
    container_name: postgres-exporter
    environment:
      - DATA_SOURCE_NAME=postgresql://hive:hive123@postgres:5432/metastore?sslmode=disable
    ports:
      - "9187:9187"
    networks:
      - data-platform
    depends_on:
      - postgres
    restart: unless-stopped
```

#### Update monitoring/prometheus/prometheus.yml:

```yaml
  - job_name: 'postgres-exporter'
    static_configs:
      - targets: ['postgres-exporter:9187']
        labels:
          service: 'postgresql-exporter'
```

**Then restart:**
```bash
docker compose up -d postgres-exporter
docker compose restart prometheus
```

---

### Option 4: Spark Metrics Sink

Configure Spark to push metrics to Prometheus.

#### Create spark/conf/metrics.properties:

```properties
# Prometheus Sink
*.sink.prometheus.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheus.path=/metrics
*.source.jvm.class=org.apache.spark.metrics.source.JvmSource

# Master metrics
master.sink.prometheus.class=org.apache.spark.metrics.sink.PrometheusServlet
master.sink.prometheus.path=/metrics/prometheus

# Worker metrics
worker.sink.prometheus.class=org.apache.spark.metrics.sink.PrometheusServlet
worker.sink.prometheus.path=/metrics/prometheus

# Application metrics
applications.sink.prometheus.class=org.apache.spark.metrics.sink.PrometheusServlet
applications.sink.prometheus.path=/metrics/prometheus
```

**Note:** Spark 3.5.0 may need additional JARs for Prometheus servlet support.

#### Alternative: Use Spark HTTP metrics endpoint

Spark exposes JSON metrics at `/metrics/json/` - you'd need a metrics adapter.

---

### Option 5: Node Exporter (System Metrics)

Monitor host system resources (CPU, disk, network of the host machine).

#### Add to docker-compose.yml:

```yaml
  node-exporter:
    image: prom/node-exporter:latest
    hostname: node-exporter
    container_name: node-exporter
    ports:
      - "9100:9100"
    command:
      - '--path.rootfs=/host'
      - '--path.procfs=/host/proc'
      - '--path.sysfs=/host/sys'
      - '--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)'
    volumes:
      - '/:/host:ro,rslave'
    networks:
      - data-platform
    restart: unless-stopped
```

#### Update Prometheus config:

```yaml
  - job_name: 'node-exporter'
    static_configs:
      - targets: ['node-exporter:9100']
        labels:
          service: 'node-exporter'
          instance: 'host-machine'
```

---

## Recommended Implementation Order

### Phase 1: Quick Wins (15 minutes)
1. ✅ **cAdvisor** - Get container metrics for all services
2. ✅ **Node Exporter** - Get host system metrics

**Impact:** Immediate visibility into resource usage across the platform

### Phase 2: Critical Data Pipeline (1-2 hours)
3. ⏳ **Kafka JMX Exporter** - Monitor message flow and consumer lag
4. ⏳ **PostgreSQL Exporter** - Monitor metastore database health

**Impact:** Full visibility into data ingestion and catalog health

### Phase 3: Processing Metrics (2-3 hours)
5. ⏳ **Spark Metrics Sink** - Monitor stream processing jobs

**Impact:** Understand processing performance and bottlenecks

### Phase 4: Optional Advanced (As Needed)
6. ⏸️ Trino JMX metrics
7. ⏸️ Hive Metastore JMX metrics
8. ⏸️ Custom application metrics

---

## Quick Start: Enable Container Monitoring Now

**This gives you immediate results with minimal effort:**

```bash
# 1. Add cAdvisor and Node Exporter to docker-compose.yml
# (Copy the YAML blocks from Option 1 and Option 5 above)

# 2. Start the new services
docker compose up -d cadvisor node-exporter

# 3. Update Prometheus config (uncomment the cAdvisor and node-exporter sections)
# Edit: monitoring/prometheus/prometheus.yml

# 4. Restart Prometheus to reload config
docker compose restart prometheus

# 5. Verify targets in Prometheus
# Go to: http://localhost:9090/targets
# You should see cadvisor and node-exporter as UP

# 6. Import Container Monitoring Dashboard in Grafana
# Dashboard ID: 193 (Docker & System Monitoring)
# Or Dashboard ID: 14282 (cAdvisor Prometheus)
```

**Within 5 minutes, you'll have:**
- CPU, memory, network metrics for all 13 containers
- Host system metrics
- Working Grafana dashboards showing real data

---

## Testing Metrics

### Check Prometheus Targets
```bash
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health, lastError: .lastError}'
```

### Query Sample Metrics
```bash
# Check what metrics are available
curl -s http://localhost:9090/api/v1/label/__name__/values | jq '.data[]' | head -20

# Query a specific metric
curl -s 'http://localhost:9090/api/v1/query?query=up' | jq '.data.result[] | {job: .metric.job, value: .value[1]}'
```

### View in Grafana Explore
1. Go to Grafana: http://localhost:3000
2. Click "Explore" (compass icon)
3. Select "Prometheus" datasource
4. Try queries:
   - `up` - See which services are being monitored
   - `container_memory_usage_bytes` - Container memory (after adding cAdvisor)
   - `minio_cluster_capacity_usable_total_bytes` - MinIO storage

---

## Grafana Dashboard Recommendations

### Currently Working Dashboards (After Basic Setup)

**For MinIO:**
- Dashboard ID: **13502** - MinIO Dashboard
- Shows: Storage usage, bandwidth, requests

**For Prometheus & Grafana:**
- Dashboard ID: **3662** - Prometheus 2.0 Overview
- Shows: Prometheus performance, scrape stats

### After Adding cAdvisor:
- Dashboard ID: **193** - Docker & System Monitoring
- Dashboard ID: **14282** - cAdvisor Prometheus
- Shows: All container metrics

### After Adding Node Exporter:
- Dashboard ID: **1860** - Node Exporter Full
- Shows: Complete host system metrics

### After Adding Kafka JMX:
- Dashboard ID: **7589** - Kafka Exporter Overview
- Dashboard ID: **11962** - Kafka Cluster Monitoring

### After Adding PostgreSQL Exporter:
- Dashboard ID: **9628** - PostgreSQL Database
- Dashboard ID: **6742** - PostgreSQL Overview

---

## Current Prometheus Configuration

The current `monitoring/prometheus/prometheus.yml` is configured for **basic working metrics only**:

✅ Enabled:
- Prometheus self-monitoring
- Grafana
- MinIO

❌ Disabled (commented out):
- Kafka (needs JMX exporter)
- PostgreSQL (needs postgres_exporter)
- Spark (needs metrics sink config)

💡 Ready to enable (commented, just need the services):
- cAdvisor
- Node Exporter

---

## Summary

**Right Now:** You have 3 working metrics sources (Prometheus, Grafana, MinIO)

**Quick Win (15 min):** Add cAdvisor + Node Exporter → Get 13 container metrics + host metrics

**Full Monitoring (2-3 hours):** Add all exporters → Complete platform observability

**Recommendation:** Start with cAdvisor. It gives you the most value for the least effort. You'll immediately see:
- Which containers are using the most CPU
- Memory usage trends
- Network traffic patterns
- Disk I/O for all services

Then add other exporters as needed based on your monitoring priorities.

---

**Next Steps:**
1. Decide which metrics are most important for your use case
2. Start with cAdvisor (easiest, high value)
3. Add Kafka metrics if you need to monitor message flow
4. Add others as needed

All the configuration examples above are ready to copy-paste into your docker-compose.yml and Prometheus config.
