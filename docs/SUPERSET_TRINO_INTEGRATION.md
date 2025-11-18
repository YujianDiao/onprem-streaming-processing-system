# Superset-Trino Integration Guide

## Overview

This guide documents the integration between Apache Superset and Trino for visualizing data from the Iceberg lakehouse. Superset provides a modern, intuitive interface for creating dashboards and exploring data stored in Iceberg tables.

**Status**: ✅ OPERATIONAL
**Last Updated**: 2025-11-17

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                   Data Visualization Stack                           │
└─────────────────────────────────────────────────────────────────────┘

USER INTERFACE
└── Apache Superset (Port 8088)
    ├── Web UI: http://localhost:8088
    ├── Credentials: admin / admin
    └── Features: Dashboards, Charts, SQL Lab

QUERY ENGINE
└── Trino (Port 8080)
    ├── Distributed SQL Engine
    ├── Iceberg Connector
    └── No Authentication (Default User: trino)

DATA STORAGE
└── Apache Iceberg Tables
    ├── Catalog: Hive Metastore
    ├── Storage: MinIO (S3-compatible)
    └── Schemas: bronze, silver, gold

METADATA
└── Hive Metastore (Port 9083)
    └── Table definitions and schemas
```

---

## Prerequisites

### System Requirements

- Docker and Docker Compose installed
- All services running:
  ```bash
  docker compose ps
  # Should show: minio, postgres, hive-metastore, trino, superset all healthy
  ```

### Data Requirements

Ensure you have data in at least one Iceberg table:
```bash
# Verify bronze data exists
docker exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.bronze.transactions;"

# Expected: Non-zero count (e.g., 20,000+ records)
```

---

## Superset Setup

### 1. Custom Docker Image

**File**: `superset/Dockerfile`

```dockerfile
FROM apache/superset:latest

# Switch to root to install packages
USER root

# Install Trino packages into the system Python dist-packages
# The venv will inherit these via site-packages
RUN pip install --target=/usr/local/lib/python3.10/dist-packages \
    --no-cache-dir trino sqlalchemy-trino

# Install into the venv site-packages directly by copying
RUN cp -r /usr/local/lib/python3.10/dist-packages/trino* /app/.venv/lib/python3.10/site-packages/ && \
    cp -r /usr/local/lib/python3.10/dist-packages/sqlalchemy_trino* /app/.venv/lib/python3.10/site-packages/ && \
    find /usr/local/lib/python3.10/dist-packages/ -maxdepth 1 -type d \( -name "*trino*" -o -name "*requests*" -o -name "*certifi*" -o -name "*urllib3*" -o -name "*charset*" -o -name "*idna*" -o -name "*pytz*" -o -name "*dateutil*" -o -name "*tzlocal*" -o -name "*orjson*" -o -name "*lz4*" -o -name "*zstandard*" \) -exec cp -r {} /app/.venv/lib/python3.10/site-packages/ \;

USER superset
```

**What This Does**:
- Extends the official Apache Superset image
- Installs Trino Python client (`trino==0.336.0`)
- Installs SQLAlchemy Trino dialect (`sqlalchemy-trino==0.5.0`)
- Copies all dependencies into Superset's virtual environment

### 2. Docker Compose Configuration

**File**: `docker-compose.yml` (excerpt)

```yaml
superset:
  build:
    context: ./superset
    dockerfile: Dockerfile
  image: custom-superset:latest
  container_name: superset
  ports:
    - "8088:8088"
  depends_on:
    - postgres
    - minio
    - hive-metastore
    - trino
  environment:
    - SUPERSET_SECRET_KEY=your_secret_key_here
  networks:
    - lakehouse
```

**Key Points**:
- Uses custom build instead of stock `apache/superset:latest`
- Exposes port 8088 for web UI
- Depends on Trino and supporting services

### 3. Build and Start Superset

```bash
# Build the custom image
docker compose build superset

# Start Superset
docker compose up -d superset

# Wait for initialization (takes ~20-30 seconds)
sleep 20

# Verify Superset is running
docker logs superset | tail -20
```

### 4. Access Superset UI

- **URL**: http://localhost:8088
- **Username**: `admin`
- **Password**: `admin`

---

## Trino Database Connection

### Step 1: Add Database Connection

1. Log into Superset UI
2. Navigate to **Settings** → **Database Connections**
3. Click **+ Database** button
4. Select **Trino** from the database type dropdown

### Step 2: Configure Connection

**Display Name**: `Trino Iceberg Lakehouse`

**SQLAlchemy URI**:
```
trino://trino@trino:8080/iceberg
```

**URI Breakdown**:
- **Protocol**: `trino://`
- **Username**: `trino` (default Trino user - **REQUIRED**)
- **Host**: `trino` (container name)
- **Port**: `8080` (Trino HTTP port)
- **Catalog**: `iceberg` (Hive-based Iceberg catalog)

### Step 3: Test Connection

Click **Test Connection** button. You should see:
```
Connection looks good!
```

If you get authentication errors:
- Ensure username `trino@` is included in the URI
- Error: `error 401: Basic authentication or X-Trino-User must be sent`
  - **Fix**: Add username to URI: `trino://trino@trino:8080/iceberg`

### Step 4: Save Connection

Click **Connect** to save the database connection.

---

## Alternative Connection Strings

### Connect to Specific Schema

```
trino://trino@trino:8080/iceberg/bronze
trino://trino@trino:8080/iceberg/silver
trino://trino@trino:8080/iceberg/gold
```

**When to Use**: If you only want to expose one schema to users.

### With Source Tag

```
trino://trino@trino:8080/iceberg?source=superset
```

**When to Use**: Helps identify queries from Superset in Trino logs.

### With Session Properties

```
trino://trino@trino:8080/iceberg?session_properties={"query_max_memory":"2GB"}
```

**When to Use**: To set Trino session-level configurations.

---

## Using SQL Lab

SQL Lab is Superset's SQL IDE for ad-hoc queries.

### Access SQL Lab

1. Click **SQL** → **SQL Lab** in top menu
2. Select **Database**: `Trino Iceberg Lakehouse`
3. Select **Schema**: `bronze`, `silver`, or `gold`

### Example Queries

#### Query Bronze Layer

```sql
-- Count total transactions
SELECT COUNT(*) as total_transactions
FROM bronze.transactions;

-- Transaction type breakdown
SELECT
    transaction_type,
    COUNT(*) as count,
    ROUND(SUM(amount), 2) as total_amount
FROM bronze.transactions
GROUP BY transaction_type
ORDER BY total_amount DESC;

-- Recent transactions
SELECT
    transaction_id,
    timestamp,
    account_id,
    transaction_type,
    amount,
    merchant,
    is_fraud
FROM bronze.transactions
ORDER BY timestamp DESC
LIMIT 100;
```

#### Query Silver Layer

```sql
-- Data quality metrics
SELECT
    validation_status,
    COUNT(*) as count
FROM silver.transactions
GROUP BY validation_status;

-- Flagged transactions
SELECT
    account_id,
    transaction_type,
    amount,
    merchant,
    transaction_timestamp,
    is_flagged
FROM silver.transactions
WHERE is_flagged = true
ORDER BY amount DESC
LIMIT 50;

-- Daily transaction volume
SELECT
    DATE(date_partition) as date,
    COUNT(*) as transactions,
    SUM(amount) as total_amount
FROM silver.transactions
GROUP BY DATE(date_partition)
ORDER BY date DESC;
```

#### Query Gold Layer

```sql
-- Account summary
SELECT
    account_id,
    transaction_date,
    total_transactions,
    total_debits,
    total_credits,
    net_amount,
    flagged_count
FROM gold.daily_account_summary
ORDER BY transaction_date DESC, net_amount DESC
LIMIT 100;

-- Top merchants by volume
SELECT
    merchant,
    transaction_date,
    total_transactions,
    total_amount,
    avg_amount,
    unique_accounts
FROM gold.merchant_summary
WHERE transaction_date = CURRENT_DATE - INTERVAL '1' DAY
ORDER BY total_amount DESC
LIMIT 20;
```

### SQL Lab Features

- **Query History**: View and re-run past queries
- **Query Results**: Download as CSV or copy to clipboard
- **Visualization**: Create quick charts from query results
- **Save Query**: Save frequently used queries
- **Query Parameters**: Use Jinja templating for dynamic queries

---

## Creating Charts

### Step 1: Explore Dataset

1. Navigate to **Datasets** tab
2. Click **+ Dataset** button
3. Select:
   - **Database**: `Trino Iceberg Lakehouse`
   - **Schema**: `bronze`, `silver`, or `gold`
   - **Table**: Select your table
4. Click **Add** to register the dataset

### Step 2: Create Chart

1. Go to **Charts** tab
2. Click **+ Chart** button
3. Select:
   - **Dataset**: Your registered dataset
   - **Chart Type**: Choose from 40+ visualization types

### Popular Chart Types

#### Time Series Charts

**Use Case**: Transaction volume over time

- **Chart Type**: Time-series Line Chart
- **Metrics**: COUNT(*)
- **Time Column**: timestamp or transaction_timestamp
- **Group By**: transaction_type

#### Bar Charts

**Use Case**: Transaction type distribution

- **Chart Type**: Bar Chart
- **Metrics**: SUM(amount)
- **Dimensions**: transaction_type

#### Pie Charts

**Use Case**: Transaction distribution

- **Chart Type**: Pie Chart
- **Metrics**: COUNT(*)
- **Dimensions**: transaction_type

#### Table View

**Use Case**: Detailed transaction list

- **Chart Type**: Table
- **Metrics**: COUNT(*), SUM(amount), AVG(amount)
- **Dimensions**: account_id, merchant, transaction_type
- **Filters**: Date range, amount thresholds

#### Big Number

**Use Case**: KPIs and metrics

- **Chart Type**: Big Number with Trendline
- **Metrics**: SUM(amount) or COUNT(*)
- **Time Column**: For trend analysis

### Step 3: Customize Chart

- **Filters**: Add WHERE clause conditions
- **Time Range**: Set date/time boundaries
- **Sorting**: Order results
- **Row Limit**: Control result size
- **Color Scheme**: Apply visual themes

### Step 4: Save Chart

1. Click **Save** button
2. Enter chart name
3. Optionally add to dashboard

---

## Creating Dashboards

### Step 1: Create Dashboard

1. Navigate to **Dashboards** tab
2. Click **+ Dashboard** button
3. Enter dashboard name: e.g., "Lakehouse Analytics"

### Step 2: Add Charts

1. Click **Edit Dashboard** button
2. Drag charts from the right panel onto the canvas
3. Resize and position charts
4. Add tabs for organizing related charts

### Step 3: Add Filters

1. Click **Add Filter** button
2. Select filter type:
   - **Time Range**: Filter by date
   - **Select Filter**: Dropdown for dimensions
   - **Text Filter**: Free-text search
3. Configure which charts the filter applies to

### Example Dashboard Layout

```
┌─────────────────────────────────────────────────────────────┐
│  Lakehouse Analytics Dashboard                              │
├─────────────────────────────────────────────────────────────┤
│  Filters: [Date Range] [Transaction Type] [Account]        │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │  Total   │  │  Total   │  │  Avg     │  │  Fraud   │   │
│  │  Tx      │  │  Amount  │  │  Amount  │  │  Count   │   │
│  │  45.2K   │  │  $2.1M   │  │  $46.50  │  │  127     │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
├─────────────────────────────────────────────────────────────┤
│  ┌────────────────────────┐  ┌─────────────────────────┐   │
│  │  Transaction Volume    │  │  Transaction Types      │   │
│  │  Over Time             │  │  Distribution           │   │
│  │  (Line Chart)          │  │  (Pie Chart)            │   │
│  │                        │  │                         │   │
│  └────────────────────────┘  └─────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Top Merchants by Transaction Volume                │   │
│  │  (Bar Chart)                                         │   │
│  │                                                      │   │
│  └─────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Recent Transactions Table                           │   │
│  │  (Paginated Table View)                              │   │
│  │                                                      │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Step 4: Publish Dashboard

1. Click **Save** button
2. Enable **Auto Refresh**: Set refresh interval (e.g., 5 minutes)
3. Set **Default Filters**: Pre-select common filter values
4. Click **Publish**

### Dashboard Features

- **Full Screen Mode**: Present dashboards without UI chrome
- **Download**: Export as PDF or PNG
- **Share**: Generate shareable links
- **Embed**: Iframe embed code for external sites
- **Versioning**: Restore previous dashboard versions

---

## Data Exploration Workflows

### Workflow 1: Exploratory Data Analysis

**Objective**: Understand transaction patterns

1. **SQL Lab**: Run ad-hoc queries to explore data
   ```sql
   SELECT
       DATE_TRUNC('hour', timestamp) as hour,
       transaction_type,
       COUNT(*) as count,
       AVG(amount) as avg_amount
   FROM bronze.transactions
   WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '24' HOUR
   GROUP BY 1, 2
   ORDER BY 1 DESC, 3 DESC;
   ```

2. **Create Chart**: Visualize hourly patterns
   - Chart Type: Heatmap
   - X-axis: hour
   - Y-axis: transaction_type
   - Metric: COUNT(*)

3. **Add to Dashboard**: Build "Hourly Patterns" dashboard

### Workflow 2: Data Quality Monitoring

**Objective**: Track data pipeline health

1. **Register Datasets**:
   - `silver.transactions`
   - `gold.daily_account_summary`
   - `gold.merchant_summary`

2. **Create Metrics**:
   - **Data Freshness**: MAX(ingestion_timestamp)
   - **Record Counts**: COUNT(*)
   - **Validation Pass Rate**: COUNT(*) WHERE validation_status = 'VALID'

3. **Build Dashboard**: "Data Quality Metrics"
   - Big Number: Latest ingestion time
   - Line Chart: Record counts over time
   - Bar Chart: Validation status distribution

### Workflow 3: Business Intelligence

**Objective**: Daily business reports

1. **Create Datasets from Gold Layer**:
   - `gold.daily_account_summary`
   - `gold.merchant_summary`

2. **Build Executive Dashboard**:
   - **KPIs**: Total transactions, revenue, average transaction value
   - **Trends**: Daily/weekly/monthly comparisons
   - **Insights**: Top merchants, top accounts, fraud metrics

3. **Schedule Reports**: Set up email reports (Superset feature)

---

## Available Iceberg Tables

### Bronze Layer: `iceberg.bronze.transactions`

**Description**: Raw transaction data ingested from Kafka

**Schema**:
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | STRING | Unique transaction identifier |
| timestamp | STRING | Transaction timestamp (raw) |
| account_id | STRING | Customer account ID |
| transaction_type | STRING | Type: PURCHASE, WITHDRAWAL, DEPOSIT, TRANSFER, PAYMENT |
| amount | DOUBLE | Transaction amount |
| currency | STRING | Currency code (e.g., USD) |
| merchant | STRING | Merchant name |
| location | STRING | Transaction location |
| customer | STRING | Customer information (JSON) |
| is_fraud | BOOLEAN | Fraud indicator |
| status | STRING | Status: COMPLETED, PENDING, FAILED |
| metadata | STRING | Additional metadata (JSON) |
| ingestion_timestamp | TIMESTAMP | When record was ingested |
| date_partition | STRING | Partition key (YYYY-MM-DD) |

**Partition**: By `date_partition` (string)
**Use Case**: Raw data exploration, data lineage

### Silver Layer: `iceberg.silver.transactions`

**Description**: Cleaned and validated transactions

**Schema**:
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | STRING | Unique transaction identifier |
| account_id | STRING | Customer account ID |
| transaction_type | STRING | Type: PURCHASE, WITHDRAWAL, DEPOSIT, TRANSFER, PAYMENT |
| amount | DECIMAL(18,2) | Transaction amount (validated) |
| currency | STRING | Currency code |
| merchant | STRING | Merchant name |
| transaction_timestamp | TIMESTAMP | Parsed timestamp |
| status | STRING | Status: COMPLETED, PENDING, FAILED |
| is_flagged | BOOLEAN | High-value transaction flag (>$10,000) |
| validation_status | STRING | Data quality status |
| processing_timestamp | TIMESTAMP | When record was processed |
| date_partition | DATE | Partition key (date type) |

**Partition**: By `days(date_partition)`
**Use Case**: Business analytics, clean data queries

### Gold Layer: `iceberg.gold.daily_account_summary`

**Description**: Daily account-level aggregations

**Schema**:
| Column | Type | Description |
|--------|------|-------------|
| account_id | STRING | Customer account ID |
| transaction_date | DATE | Aggregation date |
| total_transactions | BIGINT | Total transaction count |
| total_debits | DECIMAL(18,2) | Sum of withdrawals/payments/purchases |
| total_credits | DECIMAL(18,2) | Sum of deposits/transfers |
| debit_count | BIGINT | Count of debit transactions |
| credit_count | BIGINT | Count of credit transactions |
| net_amount | DECIMAL(18,2) | Credits - Debits |
| max_transaction | DECIMAL(18,2) | Largest transaction amount |
| min_transaction | DECIMAL(18,2) | Smallest transaction amount |
| avg_transaction | DECIMAL(18,2) | Average transaction amount |
| distinct_merchants | BIGINT | Unique merchant count |
| flagged_count | BIGINT | High-value transaction count |

**Partition**: By `days(transaction_date)`
**Use Case**: Customer analytics, account monitoring

### Gold Layer: `iceberg.gold.merchant_summary`

**Description**: Daily merchant-level analytics

**Schema**:
| Column | Type | Description |
|--------|------|-------------|
| merchant | STRING | Merchant name |
| transaction_date | DATE | Aggregation date |
| total_transactions | BIGINT | Total transaction count |
| total_amount | DECIMAL(18,2) | Sum of all transactions |
| avg_amount | DECIMAL(18,2) | Average transaction amount |
| unique_accounts | BIGINT | Unique customer count |

**Partition**: By `days(transaction_date)`
**Use Case**: Merchant analytics, business intelligence

---

## Performance Optimization

### Query Performance

**Best Practices**:

1. **Use Partition Filters**:
   ```sql
   -- Good: Uses partition pruning
   SELECT * FROM bronze.transactions
   WHERE date_partition = '2025-11-17';

   -- Bad: Full table scan
   SELECT * FROM bronze.transactions
   WHERE timestamp LIKE '2025-11-17%';
   ```

2. **Limit Result Sets**:
   ```sql
   -- Add LIMIT for exploratory queries
   SELECT * FROM silver.transactions
   LIMIT 10000;
   ```

3. **Aggregate Before Joining**:
   ```sql
   -- Good: Aggregate first
   WITH daily_counts AS (
       SELECT account_id, COUNT(*) as cnt
       FROM silver.transactions
       GROUP BY account_id
   )
   SELECT * FROM daily_counts WHERE cnt > 100;
   ```

4. **Use Gold Layer for Aggregations**:
   ```sql
   -- Good: Pre-aggregated
   SELECT * FROM gold.daily_account_summary;

   -- Bad: Aggregate on-the-fly
   SELECT account_id, COUNT(*), SUM(amount)
   FROM silver.transactions
   GROUP BY account_id;
   ```

### Superset Caching

Enable caching in Superset to improve dashboard load times:

1. Navigate to **Settings** → **Database Connections** → Edit Trino
2. **Performance** tab:
   - **Cache Timeout**: Set to 300 seconds (5 minutes)
   - **Chart Cache Timeout**: Set to 300 seconds
   - **Allow DML**: Disable (read-only connection)

### Trino Query Tuning

Monitor query performance in Trino UI:
- URL: http://localhost:8080
- View query details, execution plans, and resource usage

---

## Troubleshooting

### Issue 1: Trino Not Showing in Database Dropdown

**Symptoms**: "Trino" option not available when adding database

**Root Cause**: Trino driver not installed in Superset

**Fix**:
```bash
# Rebuild Superset with Trino driver
docker compose build superset
docker compose up -d superset

# Verify driver installation
docker exec superset python -c "import trino; print('Trino driver installed')"
```

### Issue 2: Authentication Error (401)

**Symptoms**:
```
ERROR: error 401: Basic authentication or X-Trino-User must be sent
```

**Root Cause**: Missing username in connection URI

**Fix**: Add username to URI:
```
# Wrong
trino://trino:8080/iceberg

# Correct
trino://trino@trino:8080/iceberg
```

### Issue 3: Connection Timeout

**Symptoms**: "Connection timed out" when testing database connection

**Diagnostics**:
```bash
# Check if Trino is running
docker exec trino trino --execute "SELECT 1"

# Check network connectivity from Superset
docker exec superset ping -c 3 trino

# Check Trino logs
docker logs trino | tail -50
```

**Fix**:
- Ensure Trino container is healthy
- Verify both containers are on the same network
- Check firewall rules

### Issue 4: Table Not Found

**Symptoms**:
```
ERROR: Table 'iceberg.bronze.transactions' does not exist
```

**Diagnostics**:
```bash
# Verify table exists via Trino CLI
docker exec trino trino --execute "SHOW TABLES IN iceberg.bronze;"

# Check Hive Metastore
docker logs hive-metastore | grep -i "transactions"
```

**Fix**:
- Ensure streaming job has created bronze table
- Verify Hive Metastore is accessible
- Check S3 (MinIO) has data files

### Issue 5: Slow Queries

**Symptoms**: Queries take minutes to complete

**Diagnostics**:
```bash
# Check Trino query execution
# Go to http://localhost:8080 and view query details

# Check resource usage
docker stats trino
```

**Fix**:
- Add partition filters to queries
- Use Gold layer for aggregations
- Increase Trino memory allocation in docker-compose.yml
- Limit result set size with LIMIT clause

### Issue 6: Dashboard Not Refreshing

**Symptoms**: Dashboard shows stale data

**Fix**:
1. Check **Auto Refresh** setting in dashboard
2. Clear Superset cache: **Settings** → **Clear Cache**
3. Verify upstream data is being updated:
   ```bash
   docker exec trino trino --execute \
     "SELECT MAX(ingestion_timestamp) FROM iceberg.bronze.transactions;"
   ```

---

## Security Considerations

### Network Security

- **Superset Port**: Exposed on localhost:8088 only
- **Internal Communication**: All services on private `lakehouse` network
- **Trino Access**: Only accessible within Docker network

### Authentication

**Current Setup**:
- Superset: Basic auth (admin/admin)
- Trino: No authentication (default user "trino")

**Production Recommendations**:
1. **Enable Trino Authentication**:
   - LDAP/Active Directory integration
   - OAuth 2.0 with identity provider
   - Password file authentication

2. **Secure Superset**:
   - Change default admin password
   - Enable LDAP/OAuth authentication
   - Configure HTTPS with SSL certificates
   - Implement row-level security

3. **Network Isolation**:
   - Use private subnets
   - Configure firewall rules
   - Implement VPN for remote access

### Data Access Control

**Trino Authorization** (for production):
```properties
# trino/config.properties
http-server.authentication.type=PASSWORD
access-control.name=file
access-control.config-file=/etc/trino/access-control.properties
```

**Superset Row-Level Security**:
- Define SQL filters based on user roles
- Restrict dataset access by team
- Implement column-level permissions

---

## Maintenance

### Regular Tasks

**Daily**:
- Monitor dashboard performance
- Check for failed queries in SQL Lab
- Verify data freshness in dashboards

**Weekly**:
- Review and archive old queries
- Update dashboard filters and date ranges
- Check Superset logs for errors:
  ```bash
  docker logs superset | grep -i error
  ```

**Monthly**:
- Update Superset to latest version
- Review and optimize slow queries
- Clean up unused datasets and charts

### Backup and Restore

**Backup Superset Metadata**:
```bash
# Export dashboards
docker exec superset superset export_dashboards -f /tmp/dashboards.json

# Copy from container
docker cp superset:/tmp/dashboards.json ./backup/

# Backup Superset database (SQLite)
docker cp superset:/app/superset_home/superset.db ./backup/
```

**Restore**:
```bash
# Copy dashboard backup to container
docker cp ./backup/dashboards.json superset:/tmp/

# Import dashboards
docker exec superset superset import_dashboards -p /tmp/dashboards.json
```

---

## Advanced Features

### Jinja Templating

Use Jinja templates for dynamic queries:

```sql
-- Template with date parameter
SELECT *
FROM silver.transactions
WHERE transaction_timestamp >= '{{ from_dttm }}'
  AND transaction_timestamp < '{{ to_dttm }}'
  {% if account_id %}
  AND account_id = '{{ account_id }}'
  {% endif %}
```

### Custom SQL Metrics

Define reusable metrics in dataset configuration:

```sql
-- Metric: Fraud Rate
COUNT(CASE WHEN is_fraud THEN 1 END) / COUNT(*)

-- Metric: Average Transaction Value
SUM(amount) / COUNT(*)

-- Metric: Revenue
SUM(CASE WHEN transaction_type IN ('PURCHASE', 'PAYMENT') THEN amount ELSE 0 END)
```

### Alerts and Reports

**Email Reports** (requires SMTP configuration):
1. Configure email in `superset_config.py`
2. Create report: Dashboard → **Schedule** → **Email Report**
3. Set frequency: Daily, Weekly, Monthly
4. Add recipients

**Slack Notifications** (requires Slack webhook):
1. Install Slack app for Superset
2. Configure webhook URL
3. Set up alerts for data anomalies

---

## Example Use Cases

### Use Case 1: Real-Time Transaction Monitoring

**Dashboard**: "Live Transaction Feed"

**Components**:
- Big Number: Total transactions today
- Line Chart: Transactions per minute (last hour)
- Table: Latest 100 transactions
- Auto Refresh: 30 seconds

**SQL**:
```sql
-- Latest transactions
SELECT
    transaction_id,
    transaction_timestamp,
    account_id,
    transaction_type,
    amount,
    merchant,
    is_flagged
FROM silver.transactions
WHERE transaction_timestamp > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
ORDER BY transaction_timestamp DESC
LIMIT 100;
```

### Use Case 2: Fraud Detection Dashboard

**Dashboard**: "Fraud Analytics"

**Components**:
- Big Number: Flagged transactions today
- Pie Chart: Fraud by transaction type
- Map: Fraud by location
- Table: Suspicious accounts

**SQL**:
```sql
-- High-value flagged transactions
SELECT
    account_id,
    COUNT(*) as flagged_count,
    SUM(amount) as total_flagged_amount,
    MAX(amount) as max_transaction
FROM silver.transactions
WHERE is_flagged = true
  AND transaction_timestamp > CURRENT_DATE
GROUP BY account_id
HAVING COUNT(*) > 3
ORDER BY total_flagged_amount DESC;
```

### Use Case 3: Merchant Performance Analysis

**Dashboard**: "Merchant Insights"

**Components**:
- Bar Chart: Top 20 merchants by revenue
- Line Chart: Merchant transaction trends
- Table: Merchant details with metrics

**SQL**:
```sql
-- Top merchants with growth metrics
WITH current_week AS (
    SELECT merchant, SUM(total_amount) as amount
    FROM gold.merchant_summary
    WHERE transaction_date >= CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY merchant
),
previous_week AS (
    SELECT merchant, SUM(total_amount) as amount
    FROM gold.merchant_summary
    WHERE transaction_date >= CURRENT_DATE - INTERVAL '14' DAY
      AND transaction_date < CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY merchant
)
SELECT
    c.merchant,
    c.amount as current_week_revenue,
    p.amount as previous_week_revenue,
    ROUND((c.amount - p.amount) / p.amount * 100, 2) as growth_pct
FROM current_week c
LEFT JOIN previous_week p ON c.merchant = p.merchant
ORDER BY c.amount DESC
LIMIT 20;
```

---

## Resources

### Documentation

- **Superset Docs**: https://superset.apache.org/docs/intro
- **Trino Docs**: https://trino.io/docs/current/
- **Iceberg Docs**: https://iceberg.apache.org/docs/latest/
- **SQLAlchemy Trino**: https://github.com/trinodb/sqlalchemy-trino

### Internal Documentation

- **Pipeline Overview**: `docs/PIPELINE_UPDATES_SUMMARY.md`
- **Streaming Fix**: `docs/STREAMING_PIPELINE_FIX.md`
- **Batch Updates**: `docs/BATCH_AGGREGATIONS_UPDATE.md`

### Getting Help

**Check Logs**:
```bash
# Superset logs
docker logs superset -f

# Trino logs
docker logs trino -f

# Hive Metastore logs
docker logs hive-metastore -f
```

**Access Trino Web UI**:
- URL: http://localhost:8080
- View query history, execution plans, cluster status

**Verify Data Pipeline**:
```bash
# Check streaming job status
docker exec spark-master ps aux | grep kafka_to_iceberg

# Query latest data
docker exec trino trino --execute \
  "SELECT MAX(ingestion_timestamp) FROM iceberg.bronze.transactions;"
```

---

## Next Steps

### Immediate Actions

1. ✅ Create your first database connection
2. ✅ Explore data using SQL Lab
3. ✅ Register datasets for all Iceberg tables
4. ✅ Create 3-5 basic charts
5. ✅ Build your first dashboard

### Short Term Enhancements

1. **Add More Dashboards**:
   - Customer segmentation analysis
   - Daily operations report
   - Data quality monitoring

2. **Implement Alerts**:
   - Set up email reports for executives
   - Configure Slack notifications for anomalies

3. **User Management**:
   - Create roles: Analyst, Manager, Executive
   - Configure row-level security
   - Implement dataset permissions

### Long Term Goals

1. **Advanced Analytics**:
   - Integrate machine learning models
   - Predictive analytics dashboards
   - Customer churn analysis

2. **Data Catalog**:
   - Document all datasets and metrics
   - Create data dictionary
   - Implement data lineage tracking

3. **Performance Optimization**:
   - Implement aggressive caching
   - Pre-compute common aggregations
   - Optimize Trino cluster sizing

---

**Document Status**: Complete
**Integration Status**: ✅ Operational
**Last Verified**: 2025-11-17 13:15 UTC
