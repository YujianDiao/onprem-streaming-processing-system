# Monitoring Quick Reference Card

## Status: ✅ ALL SERVICES MONITORED (9/9 UP)

---

## Access URLs

| Service | URL | Login |
|---------|-----|-------|
| **Prometheus** | http://localhost:9090 | None |
| **Grafana** | http://localhost:3000 | admin/admin |
| **cAdvisor** | http://localhost:8085 | None |

---

## Quick Commands

### Check All Targets
```bash
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'
```

### Check Service Availability
```bash
curl -s 'http://localhost:9090/api/v1/query?query=up' | jq '.data.result[] | {job: .metric.job, status: .value[1]}'
```

### Restart Monitoring Services
```bash
docker compose restart prometheus grafana cadvisor node-exporter postgres-exporter
docker compose restart kafka-jmx-exporter-1 kafka-jmx-exporter-2 kafka-jmx-exporter-3
```

---

## Import These Dashboards in Grafana

Go to Grafana → + → Import → Enter ID:

1. **193** - Docker & System Monitoring (START HERE)
2. **7589** - Kafka Exporter Overview
3. **9628** - PostgreSQL Database
4. **13502** - MinIO Dashboard
5. **1860** - Node Exporter Full
6. **3662** - Prometheus 2.0 Overview

---

## Useful Queries

### Container Memory (Top 5)
```promql
topk(5, container_memory_usage_bytes{name!=""})
```

### Kafka Messages Rate
```promql
sum(rate(kafka_server_brokertopicmetrics_messagesinpersec[5m])) by (broker_host)
```

### PostgreSQL Connections
```promql
pg_stat_database_numbackends{datname="metastore"}
```

### MinIO Storage Used
```promql
minio_cluster_capacity_usable_total_bytes
```

### Host CPU %
```promql
100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)
```

---

## What's Monitored

✅ **Prometheus** - Self-monitoring
✅ **Grafana** - Dashboard performance
✅ **cAdvisor** - ALL 20+ containers (CPU, memory, network)
✅ **Node Exporter** - Host system (CPU, RAM, disk)
✅ **MinIO** - Storage, bandwidth, requests
✅ **PostgreSQL** - 31 connections, queries, performance
✅ **Kafka Broker 1** - 21 leaders, throughput
✅ **Kafka Broker 2** - 21 leaders, throughput
✅ **Kafka Broker 3** - 22 leaders, throughput

**Total: 500+ metric series**

---

## Troubleshooting

**Target is DOWN:**
```bash
docker compose logs [service-name]
docker compose restart [service-name]
docker compose restart prometheus
```

**No data in Grafana:**
1. Check datasource: Configuration → Data Sources → Test
2. Check time range: Top right, try "Last 5 minutes"
3. Try query in Explore: `up`

**Kafka JMX not working:**
```bash
docker compose logs kafka-jmx-exporter-1
docker exec kafka-1 netstat -tulpn | grep 9101
```

---

## Files Modified

1. `docker-compose.yml` - Added 7 monitoring services
2. `monitoring/prometheus/prometheus.yml` - 9 scrape targets
3. `monitoring/jmx-exporter/kafka-{1,2,3}-config.yml` - JMX configs

---

## Next Steps

1. ✅ Open Grafana: http://localhost:3000
2. ✅ Import Dashboard 193 (Docker monitoring)
3. ✅ See your metrics live!
4. ⏭️ Import more dashboards as needed
5. ⏭️ Set up alerts (optional)

---

**Status:** 🟢 MONITORING COMPLETE - Enjoy your dashboards! 📊
