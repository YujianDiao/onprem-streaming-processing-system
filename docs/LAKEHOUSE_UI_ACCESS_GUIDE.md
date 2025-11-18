# Lakehouse Platform - UI Access Guide

## All Available Web Interfaces

### 1. **Apache Superset** - Data Visualization & BI Platform ⭐ **RECOMMENDED FOR TABLE BROWSING**

**URL:** http://localhost:8088

**Login:**
- Username: `admin`
- Password: `admin`

**Features:**
- Rich table browsing and visualization
- SQL Lab for interactive queries
- Dashboard creation
- Chart builder
- Supports Trino/Iceberg integration

**How to Connect to Trino:**
1. Click on **Settings** → **Database Connections**
2. Click **+ Database**
3. Select **Trino** from supported databases
4. Fill in connection details:
   - Host: `trino`
   - Port: `8080`
   - Catalog: `iceberg`
   - Schema: `bronze`
5. Click **Connect**

**Then Query Your Tables:**
- Go to **SQL Lab** → **SQL Editor**
- Select your Trino connection
- Run queries like:
  ```sql
  SELECT * FROM iceberg.bronze.transactions LIMIT 100;

  SELECT
    transaction_type,
    COUNT(*) as count,
    AVG(amount) as avg_amount
  FROM iceberg.bronze.transactions
  GROUP BY transaction_type;
  ```

---

### 2. **MinIO Console** - Object Storage Browser

**URL:** http://localhost:9001

**Login:**
- Access Key: `minioadmin`
- Secret Key: `minioadmin`

**Features:**
- Browse lakehouse bucket
- View Iceberg table directories
- See parquet data files
- View metadata files
- Download files
- Bucket management

**What You'll See:**
```
lakehouse/
├── bronze/
│   └── transactions/
│       ├── data/
│       │   └── date_partition=2025-11-16/
│       │       └── *.parquet files
│       └── metadata/
│           ├── *.avro files
│           └── v*.metadata.json files
└── checkpoints/
    └── bronze_transactions/
```

---

### 3. **Trino Web UI** - Query Engine Dashboard

**URL:** http://localhost:8086

**Features:**
- View running queries
- Query history
- Query details and execution plans
- Worker/coordinator status
- No login required

**Note:** Basic UI, doesn't have table browser. Use SQL CLI or Superset for querying.

---

### 4. **Spark Master UI** - Spark Cluster Dashboard

**URL:** http://localhost:8888

**Features:**
- View running applications
- See completed applications
- Application details and logs
- Worker nodes status
- Resource usage

**Current Running Job:**
- Name: `KafkaToIcebergStreaming`
- Status: RUNNING
- Processes data every 30 seconds from Kafka to Iceberg

---

### 5. **Grafana** - Monitoring Dashboards

**URL:** http://localhost:3000

**Login:**
- Username: `admin`
- Password: `admin`

**Features:**
- Kafka cluster metrics
- Container resource usage (cAdvisor)
- Host system metrics (Node Exporter)
- PostgreSQL database metrics
- Custom dashboards

**Available Dashboards:**
- Kafka JMX Metrics (custom imported)
- System metrics

---

### 6. **Prometheus** - Metrics Database

**URL:** http://localhost:9090

**Features:**
- Raw metrics explorer
- Query metrics with PromQL
- Targets status
- Alerts configuration

**Current Targets (9/9 UP):**
- Prometheus itself
- MinIO
- cAdvisor
- Node Exporter
- PostgreSQL Exporter
- Kafka Broker 1, 2, 3 (via JMX Exporter)

---

## Quick Reference Table

| Service | URL | Login | Best For |
|---------|-----|-------|----------|
| **Superset** | http://localhost:8088 | admin/admin | **Table browsing, SQL queries, dashboards** |
| **MinIO** | http://localhost:9001 | minioadmin/minioadmin | Raw file storage viewing |
| **Trino** | http://localhost:8086 | None | Query monitoring |
| **Spark** | http://localhost:8888 | None | Streaming job status |
| **Grafana** | http://localhost:3000 | admin/admin | Infrastructure monitoring |
| **Prometheus** | http://localhost:9090 | None | Metrics exploration |

---

## Common Tasks

### View Tables in the Lakehouse

**Method 1: Superset (Easiest)**
1. Open http://localhost:8088
2. Login (admin/admin)
3. Go to SQL Lab → SQL Editor
4. Connect to Trino
5. Query: `SELECT * FROM iceberg.bronze.transactions LIMIT 10`

**Method 2: Trino CLI**
```bash
docker exec -it trino trino --catalog iceberg --schema bronze

# Then run SQL:
SELECT * FROM transactions LIMIT 10;
```

**Method 3: Browse Files in MinIO**
1. Open http://localhost:9001
2. Login (minioadmin/minioadmin)
3. Navigate to: Buckets → lakehouse → bronze → transactions → data

---

### Monitor Streaming Job

1. Open Spark UI: http://localhost:8888
2. Click on "KafkaToIcebergStreaming" application
3. Check:
   - **Streaming** tab: Batch processing times
   - **Jobs** tab: Completed micro-batches
   - **Executors** tab: Resource usage

---

### Check Kafka Metrics

1. Open Grafana: http://localhost:3000
2. Login (admin/admin)
3. Go to Dashboards
4. Select "Kafka JMX Metrics"
5. View:
   - Broker throughput
   - Topic partitions
   - Under-replicated partitions
   - Messages per second

---

## Troubleshooting

### Can't Access UI
```bash
# Check if container is running
docker compose ps

# Check specific service
docker compose ps superset

# View logs
docker logs superset
```

### Superset Can't Connect to Trino
- Ensure Trino is running: `docker compose ps trino`
- Use hostname `trino` (not `localhost`)
- Port: `8080` (internal port)
- Check network: both should be in `data-platform` network

### Tables Not Showing in Superset
1. First verify table exists in Trino CLI:
   ```bash
   docker exec trino trino --execute "SHOW TABLES IN iceberg.bronze"
   ```
2. Refresh Superset database connection
3. Clear Superset cache (Settings → Database → Edit → Refresh metadata)

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    User Interfaces (Browser)                 │
├───────────┬──────────┬──────────┬──────────┬────────────────┤
│ Superset  │  MinIO   │  Trino   │  Spark   │    Grafana     │
│  :8088    │  :9001   │  :8086   │  :8888   │     :3000      │
└─────┬─────┴────┬─────┴────┬─────┴────┬─────┴────────┬───────┘
      │          │          │          │              │
      └──────────┴──────────┴──────────┴──────────────┘
                           │
              ┌────────────┴────────────┐
              │   Data Platform Layer    │
              ├─────────────────────────┤
              │  Trino (Query Engine)   │
              │  Spark (Processing)     │
              │  Kafka (Streaming)      │
              │  Iceberg (Table Format) │
              │  MinIO (Storage)        │
              │  Hive Metastore         │
              │  PostgreSQL (Metadata)  │
              └─────────────────────────┘
```

---

**Status:** ✅ All UIs are accessible and operational!
