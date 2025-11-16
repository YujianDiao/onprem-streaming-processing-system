# Monitoring Setup Guide - Prometheus & Grafana

## Overview

**Current Status:**
- ✅ Prometheus configuration exists (`monitoring/prometheus/prometheus.yml`)
- ✅ Grafana provisioning directories created
- ⚠️ Need to add Grafana datasource and dashboard configurations
- ⚠️ Prometheus config updated for Spark worker ports (8091, 8092)

---

## What's Already Configured

### ✅ Prometheus is Pre-Configured to Monitor:

1. **Kafka Cluster** (3 brokers)
   - kafka-1:9101
   - kafka-2:9102
   - kafka-3:9103

2. **PostgreSQL** (Hive Metastore DB)
   - postgres:5432

3. **MinIO** (Object Storage)
   - minio:9000

4. **Spark Cluster**
   - spark-master:8080
   - spark-worker-1:8091 (✅ updated)
   - spark-worker-2:8092 (✅ updated)

5. **Grafana**
   - grafana:3000

6. **Prometheus itself**
   - localhost:9090

---

## Quick Start (Minimal Configuration)

### Option 1: Start Without Pre-Configuration (Easiest)

```bash
# 1. Start Prometheus and Grafana
docker compose up -d prometheus grafana

# 2. Access Grafana
open http://localhost:3000

# 3. Login with default credentials
Username: admin
Password: admin

# 4. Prometheus is already configured as datasource!
# The datasource will be auto-configured via the volumes
```

**What you need to do manually:**
- Change admin password on first login
- Import dashboards (see below)

---

## Complete Configuration Setup

### Step 1: Create Grafana Datasource Configuration

```bash
# Create datasources.yml
cat > monitoring/grafana/provisioning/datasources.yml <<'EOF'
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
EOF
```

### Step 2: Create Grafana Dashboard Provisioning

```bash
# Create dashboards.yml
cat > monitoring/grafana/provisioning/dashboards.yml <<'EOF'
apiVersion: 1

providers:
  - name: 'Data Platform Dashboards'
    orgId: 1
    folder: 'Data Platform'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards
      foldersFromFilesStructure: true
EOF
```

### Step 3: Fix Permissions (if needed)

```bash
# Make files readable by Grafana container
chmod 644 monitoring/grafana/provisioning/*.yml
chmod 755 monitoring/grafana/provisioning
chmod 755 monitoring/grafana/dashboards
```

### Step 4: Start Services

```bash
# Start Prometheus and Grafana
docker compose up -d prometheus grafana

# Check logs
docker compose logs prometheus
docker compose logs grafana

# Verify they're healthy
docker compose ps prometheus grafana
```

---

## Accessing the Services

### Prometheus
- **URL:** http://localhost:9090
- **Purpose:** Metrics database and query interface

**What to check:**
1. Go to Status → Targets
2. Verify all scrape targets are "UP"
3. Expected targets:
   - prometheus (1/1 up)
   - kafka-broker-1, 2, 3 (3/3 up)
   - postgres (1/1 up)
   - minio (1/1 up)
   - spark-master (1/1 up)
   - spark-workers (2/2 up)
   - grafana (1/1 up)

### Grafana
- **URL:** http://localhost:3000
- **Default Credentials:**
  - Username: `admin`
  - Password: `admin`

**What to do:**
1. Login with default credentials
2. Change password when prompted
3. Verify Prometheus datasource is connected (Configuration → Data Sources)
4. Import dashboards (see below)

---

## Important Note About Service Metrics

### ⚠️ Not All Services Expose Prometheus Metrics by Default

**Services with Native Prometheus Support:**
- ✅ **Kafka** - JMX metrics exposed (ports 9101-9103)
- ✅ **MinIO** - Built-in Prometheus endpoint
- ✅ **Prometheus** - Self-monitoring
- ✅ **Grafana** - Internal metrics

**Services Requiring Exporters:**
- ⚠️ **PostgreSQL** - Needs postgres_exporter (not installed)
- ⚠️ **Spark** - Has metrics API but may need configuration
- ⚠️ **Trino** - Has JMX metrics but needs exporter

**Services Without Direct Metrics:**
- ❌ **Hive Metastore** - Would need JMX exporter
- ❌ **Schema Registry** - Would need custom exporter

---

## What You'll Actually Monitor (Out of the Box)

### ✅ Available Metrics:

1. **Kafka Metrics** (via JMX)
   - Broker health
   - Topic throughput
   - Consumer lag
   - Partition counts
   - Network I/O

2. **MinIO Metrics**
   - Disk usage
   - Request rates
   - Bandwidth
   - Object counts

3. **Container Metrics** (if you add cAdvisor)
   - CPU usage
   - Memory usage
   - Network I/O
   - Disk I/O

---

## Recommended Dashboards to Import

### Pre-Built Grafana Dashboards

Visit Grafana Dashboard Library: https://grafana.com/grafana/dashboards/

**Kafka Monitoring:**
- Dashboard ID: **7589** - Kafka Exporter Overview
- Dashboard ID: **11962** - Kafka Cluster Monitoring

**MinIO Monitoring:**
- Dashboard ID: **13502** - MinIO Dashboard

**Docker/Container Monitoring:**
- Dashboard ID: **193** - Docker & System Monitoring

**How to Import:**
1. In Grafana: Click "+" → Import
2. Enter Dashboard ID
3. Select Prometheus as datasource
4. Click "Import"

---

## Enhanced Monitoring (Optional)

### Add More Exporters for Complete Coverage

#### 1. PostgreSQL Exporter

Add to `docker-compose.yml`:

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
    restart: unless-stopped
```

Add to `prometheus.yml`:

```yaml
  - job_name: 'postgres-exporter'
    static_configs:
      - targets: ['postgres-exporter:9187']
        labels:
          service: 'postgresql-exporter'
```

#### 2. Container Metrics (cAdvisor)

Add to `docker-compose.yml`:

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
```

Add to `prometheus.yml`:

```yaml
  - job_name: 'cadvisor'
    static_configs:
      - targets: ['cadvisor:8080']
        labels:
          service: 'container-metrics'
```

#### 3. Node Exporter (System Metrics)

Add to `docker-compose.yml`:

```yaml
  node-exporter:
    image: prom/node-exporter:latest
    hostname: node-exporter
    container_name: node-exporter
    ports:
      - "9100:9100"
    command:
      - '--path.rootfs=/host'
    volumes:
      - '/:/host:ro,rslave'
    networks:
      - data-platform
    restart: unless-stopped
```

Add to `prometheus.yml`:

```yaml
  - job_name: 'node-exporter'
    static_configs:
      - targets: ['node-exporter:9100']
        labels:
          service: 'node-exporter'
```

---

## Quick Testing

### Test Prometheus is Scraping

```bash
# Check Prometheus targets
curl http://localhost:9090/api/v1/targets | jq

# Query a metric
curl 'http://localhost:9090/api/v1/query?query=up' | jq
```

### Test Grafana Datasource

```bash
# Check Grafana health
curl http://localhost:3000/api/health

# List datasources (after login)
curl -u admin:admin http://localhost:3000/api/datasources
```

---

## Troubleshooting

### Prometheus Not Scraping Targets

```bash
# Check Prometheus logs
docker compose logs prometheus

# Check if targets are reachable
docker exec prometheus wget -O- http://kafka-1:9101/metrics
docker exec prometheus wget -O- http://minio:9000/minio/v2/metrics/cluster
```

### Grafana Can't Connect to Prometheus

```bash
# Verify they're on same network
docker network inspect data-platform | grep -E "prometheus|grafana"

# Test from Grafana container
docker exec grafana wget -O- http://prometheus:9090/api/v1/query?query=up
```

### No Metrics Appearing

1. **Check Prometheus targets:** http://localhost:9090/targets
2. **Verify scrape intervals:** Some services may take 15-30s to appear
3. **Check service logs:** Some services may not expose metrics by default

---

## Summary

### ✅ What's Already Working:

1. **Prometheus configuration exists** with correct scrape targets
2. **Spark worker ports fixed** (8091, 8092)
3. **Grafana directories created** and ready

### 🔧 What You Need to Do:

**Minimal Setup (5 minutes):**
```bash
# 1. Start services
docker compose up -d prometheus grafana

# 2. Access Grafana: http://localhost:3000 (admin/admin)

# 3. Import dashboards for Kafka and MinIO
```

**Full Setup (15 minutes):**
```bash
# 1. Create Grafana config files (see Step 1 & 2 above)

# 2. Start services
docker compose up -d prometheus grafana

# 3. Verify Prometheus targets: http://localhost:9090/targets

# 4. Login to Grafana and import dashboards

# 5. (Optional) Add postgres-exporter and cAdvisor for more metrics
```

---

## Next Steps

1. ✅ **Start Prometheus & Grafana:**
   ```bash
   docker compose up -d prometheus grafana
   ```

2. ✅ **Access Grafana:**
   - URL: http://localhost:3000
   - Login: admin/admin
   - Change password

3. ✅ **Verify Prometheus:**
   - URL: http://localhost:9090
   - Check: Status → Targets

4. ✅ **Import Dashboards:**
   - Kafka Dashboard: ID 7589
   - MinIO Dashboard: ID 13502

5. ⏭️ **Optional: Add More Exporters**
   - postgres-exporter
   - cAdvisor
   - node-exporter

---

**Status:** 🟢 **Prometheus & Grafana are ready to start with minimal configuration!**

The basic monitoring will work out of the box for:
- Kafka cluster health
- MinIO storage metrics
- Basic service availability

Advanced metrics (PostgreSQL, detailed Spark metrics, container metrics) require additional exporters.
