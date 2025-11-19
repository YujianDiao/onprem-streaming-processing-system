# DataOps Engineer - Interview Questions & Answers

**Role:** DataOps Engineer
**Platform:** On-Premise Streaming Data Lakehouse
**Date:** 2025-11-19

---

## Table of Contents

1. [Infrastructure & Operations](#infrastructure--operations)
2. [Monitoring & Observability](#monitoring--observability)
3. [CI/CD & Automation](#cicd--automation)
4. [Troubleshooting & Incident Response](#troubleshooting--incident-response)
5. [Performance Optimization](#performance-optimization)
6. [Security & Compliance](#security--compliance)
7. [Disaster Recovery & Backup](#disaster-recovery--backup)
8. [Cost Optimization](#cost-optimization)

---

## Infrastructure & Operations

### Q1: Describe the infrastructure architecture of our data lakehouse platform. What are the key components and how do they interact?

**Answer:**

Our infrastructure is a containerized data lakehouse platform running on Docker Compose with the following key components:

**Architecture Layers:**

1. **Ingestion Layer:**
   - **Kafka Cluster (3 brokers):** KRaft mode (no Zookeeper), handling event streaming
   - **Schema Registry:** Managing Avro/JSON schemas for data validation
   - **Kafka UI:** Web-based monitoring and management interface

2. **Processing Layer:**
   - **Spark Cluster:** 1 Master + 2 Workers (4GB RAM, 2 cores each)
   - **Streaming Job:** Continuous processing from Kafka to Iceberg (30-second micro-batches)
   - **Batch Jobs:** Bronze→Silver→Gold transformations (on-demand)

3. **Storage Layer:**
   - **MinIO:** S3-compatible object storage for Iceberg data files
   - **Apache Iceberg:** ACID-compliant table format with time travel capabilities
   - **PostgreSQL:** Hive Metastore backend for table metadata

4. **Metadata Layer:**
   - **Hive Metastore:** Central catalog for Iceberg tables (schemas, partitions, snapshots)

5. **Query & Analytics Layer:**
   - **Trino:** Distributed SQL query engine for analytics
   - **Superset:** BI visualization and dashboard platform

6. **Monitoring Layer:**
   - **Prometheus:** Metrics collection (15-second scrape interval)
   - **Grafana:** Dashboard visualization
   - **JMX Exporters:** Kafka-specific metrics
   - **Node/Container Exporters:** System-level metrics

**Component Interactions:**
```
Data Generator → Kafka → Spark Streaming → Iceberg (MinIO)
                                              ↓
                                    Hive Metastore (PostgreSQL)
                                              ↓
                                    Trino ← Superset (Dashboards)
                                              ↓
                         Prometheus/Grafana (Monitoring)
```

**Network:**
- Single Docker network: `data-platform` (172.20.0.0/16)
- All inter-service communication via container DNS names
- External access through mapped ports (localhost)

**Key Operational Characteristics:**
- No orchestration layer (Airflow) - continuous streaming handles real-time
- Medallion architecture (Bronze/Silver/Gold layers)
- Exactly-once semantics via Spark checkpointing
- All services containerized for portability

---

### Q2: We're running Kafka in KRaft mode instead of using Zookeeper. Explain the advantages and what operational considerations we need to be aware of.

**Answer:**

**Advantages of KRaft Mode:**

1. **Simplified Architecture:**
   - Eliminates Zookeeper dependency (reduces component count by 3+ containers)
   - Fewer failure modes to manage
   - Simpler deployment and upgrade procedures
   - Reduced operational complexity

2. **Improved Performance:**
   - Lower latency for metadata operations (controller elections, partition assignments)
   - Faster recovery during broker failures
   - More efficient metadata storage (Raft log vs. Zookeeper tree)
   - Single consensus protocol instead of two

3. **Better Scalability:**
   - No Zookeeper bottleneck for large clusters
   - Controller can handle more partitions per cluster
   - Faster metadata propagation to brokers

4. **Operational Benefits:**
   - Single set of configurations to manage
   - Unified monitoring (no separate Zookeeper metrics)
   - Easier disaster recovery (single data store)

**Operational Considerations:**

1. **Controller Quorum Configuration:**
   ```properties
   # Critical: All brokers must agree on controller voters
   process.roles=broker,controller
   controller.quorum.voters=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093

   # Quorum size: (N/2 + 1) must be available
   # For 3 nodes: Can tolerate 1 failure
   # For 5 nodes: Can tolerate 2 failures
   ```

2. **Cluster ID Management:**
   - **First-time setup:** Generate cluster ID with `kafka-storage.sh random-uuid`
   - **Format storage:** `kafka-storage.sh format -t <cluster-id> -c /config/server.properties`
   - **Critical:** Never change cluster ID after initialization (data loss)

3. **Listener Configuration:**
   ```properties
   # Separate listeners for controller and broker traffic
   listeners=PLAINTEXT://:29092,EXTERNAL://:9092,CONTROLLER://:9093
   controller.listener.names=CONTROLLER
   inter.broker.listener.name=PLAINTEXT
   ```
   - Controller listener MUST be separate from broker listeners
   - Controller traffic is internal only (not exposed externally)

4. **Monitoring:**
   - Watch `kafka.controller:type=KafkaController,name=ActiveControllerCount`
     - Should be 1 in the cluster (exactly one active controller)
   - Monitor `kafka.controller:type=KafkaController,name=ControllerState`
     - Values: 0=Not controller, 1=Active controller
   - Alert on controller elections (frequent elections indicate instability)

5. **Upgrade Considerations:**
   - KRaft is production-ready as of Kafka 3.3+
   - Cannot downgrade to Zookeeper mode after KRaft initialization
   - Rolling upgrades: Update one broker at a time, wait for replication
   - Migration from Zookeeper: Use `kafka-metadata-migration` tool (not applicable here)

6. **Backup & Recovery:**
   - **Metadata location:** `/var/lib/kafka/data/__cluster_metadata-0/`
   - **Backup strategy:** Volume snapshots (not hot-swappable like Zookeeper)
   - **Recovery:** Restore from volume backup, ensure quorum availability

7. **Limitations (as of Kafka 7.5.0):**
   - Some admin operations may differ from Zookeeper mode
   - Feature parity reached in Kafka 3.5+
   - Our version (7.5.0 = Confluent packaging of Kafka 3.5.0) has full support

**Operational Commands:**

```bash
# Check controller status
docker exec kafka1 kafka-metadata-shell --snapshot /var/lib/kafka/data/__cluster_metadata-0/metadata.log

# Check cluster metadata
docker exec kafka1 kafka-cluster cluster-id --bootstrap-server kafka1:29092

# Monitor controller elections
docker exec kafka1 kafka-broker-api-versions --bootstrap-server kafka1:29092
```

**Failure Scenarios:**

| Scenario | Impact | Recovery |
|----------|--------|----------|
| 1 broker down (3-node cluster) | Controller quorum maintained, cluster operational | Automatic failover, restart failed broker |
| 2 brokers down | Controller quorum lost, cluster unavailable | Manual restart of at least 1 broker to restore quorum |
| Metadata corruption | Cluster inoperable | Restore from volume backup |

**Best Practices for Our Setup:**
- Monitor controller election frequency (should be rare)
- Set up alerts for `ActiveControllerCount != 1`
- Regular volume backups of `/var/lib/kafka/data`
- Document cluster ID in runbook (needed for disaster recovery)
- Test failover scenarios in staging before production

---

### Q3: How would you scale our current single-server deployment to a multi-node cluster? What changes are required?

**Answer:**

**Scaling Strategy: Single Server → Multi-Node Cluster**

**Phase 1: Infrastructure Preparation**

1. **Server Provisioning (Minimum 5 nodes for HA):**
   ```
   Node 1-3: Kafka Brokers + Spark Workers
   Node 4: Trino Coordinator + Hive Metastore + PostgreSQL
   Node 5: MinIO (or distributed MinIO on nodes 1-4)

   Recommended Hardware per Node:
   - CPU: 16+ cores
   - RAM: 64 GB
   - Storage: 1 TB SSD
   - Network: 10 Gbps
   ```

2. **Network Configuration:**
   ```yaml
   # Overlay network (Docker Swarm) or CNI (Kubernetes)
   network:
     name: data-platform
     driver: overlay  # For Docker Swarm
     attachable: true
     ipam:
       config:
         - subnet: 10.0.0.0/16

   # Or use Kubernetes networking:
   # - Service mesh (Istio) for inter-service communication
   # - Ingress controller for external access
   # - Network policies for segmentation
   ```

3. **DNS & Service Discovery:**
   - **Docker Swarm:** Built-in DNS
   - **Kubernetes:** CoreDNS + Services
   - **Manual:** Consul or etcd for service registry

**Phase 2: Component Migration**

**2.1 Kafka Cluster (Horizontal Scaling):**

```yaml
# Change from 3 brokers on 1 node to 5 brokers on 5 nodes
# Current: kafka1, kafka2, kafka3 (same host)
# Target: kafka1 (node1), kafka2 (node2), ..., kafka5 (node5)

services:
  kafka-1:
    # ... existing config ...
    deploy:
      placement:
        constraints:
          - node.hostname == kafka-node-1
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-node-1:9093,2@kafka-node-2:9093,3@kafka-node-3:9093,4@kafka-node-4:9093,5@kafka-node-5:9093
      # Increase replication factor for production
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3

  kafka-2:
    # ... repeat for nodes 2-5 ...
    deploy:
      placement:
        constraints:
          - node.hostname == kafka-node-2
```

**Migration Steps:**
1. Add new brokers (kafka4, kafka5) to existing cluster
2. Reassign partitions to new brokers: `kafka-reassign-partitions.sh`
3. Verify replication: `kafka-topics.sh --describe`
4. Update replication factor: `kafka-configs.sh --alter`

**2.2 Spark Cluster (Add Workers):**

```yaml
# Scale from 2 workers (1 node) to 10 workers (5 nodes)
services:
  spark-worker-1:
    # ... existing config ...
    deploy:
      replicas: 2  # 2 workers per node
      placement:
        constraints:
          - node.hostname == spark-node-1
    environment:
      SPARK_WORKER_CORES: 8  # More cores available
      SPARK_WORKER_MEMORY: 32G  # More memory

  # Workers on other nodes
  spark-worker-2:
    # ... on spark-node-2 ...
  # ... workers 3-10 ...
```

**Streaming Job Configuration Changes:**
```python
# Update Spark submit configuration
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \  # Change from client to cluster mode
  --driver-memory 8g \      # Increase from 2g
  --executor-memory 16g \   # Increase from 3g
  --executor-cores 4 \      # Increase from 2
  --total-executor-cores 40 \  # 10 workers × 4 cores
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=5 \
  --conf spark.dynamicAllocation.maxExecutors=20 \
  # ... rest of config ...
```

**2.3 MinIO Distributed Mode (4+ nodes):**

```yaml
# Distributed MinIO requires 4-16 nodes, even number of drives
# Configuration: 4 nodes × 4 drives/node = 16 drives

services:
  minio-1:
    image: minio/minio:latest
    command: server http://minio-{1...4}/data{1...4}
    deploy:
      placement:
        constraints:
          - node.hostname == storage-node-1
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
      MINIO_STORAGE_CLASS_STANDARD: EC:2  # Erasure coding (2 parity drives)

  # Repeat for minio-2, minio-3, minio-4
```

**Migration Steps:**
1. Deploy distributed MinIO on new nodes
2. Use `mc mirror` to copy data from old to new cluster:
   ```bash
   mc alias set old-minio http://old-minio:9000 minioadmin minioadmin
   mc alias set new-minio http://minio-load-balancer:9000 minioadmin minioadmin
   mc mirror --preserve old-minio/lakehouse new-minio/lakehouse
   ```
3. Update S3A endpoints in Spark/Trino configs
4. Verify data integrity with checksums

**2.4 PostgreSQL High Availability:**

```yaml
# Option 1: Primary-Standby with Patroni
services:
  postgres-1:
    image: postgres:15
    # ... Patroni for automatic failover ...
    deploy:
      placement:
        constraints:
          - node.hostname == db-node-1
    environment:
      PATRONI_SCOPE: metastore-cluster
      PATRONI_NAME: postgres-1

  postgres-2:
    # Standby replica on db-node-2

  etcd:  # Required for Patroni consensus
    # 3-node etcd cluster for Patroni

  haproxy:  # Load balancer for PostgreSQL
    # Route writes to primary, reads to standby

# Option 2: PostgreSQL Streaming Replication (simpler)
services:
  postgres-primary:
    # ... primary config ...
  postgres-standby:
    # ... standby config with streaming replication ...
```

**Migration Steps:**
1. Set up replication from existing PostgreSQL to new primary
2. Promote new primary when synchronized
3. Update Hive Metastore connection string
4. Configure automatic failover (Patroni or pgpool)

**2.5 Trino Distributed Query Engine:**

```yaml
# Separate coordinator from workers
services:
  trino-coordinator:
    image: trinodb/trino:latest
    deploy:
      placement:
        constraints:
          - node.hostname == query-node-1
    configs:
      - source: coordinator-config
        target: /etc/trino/config.properties
    # config.properties:
    # coordinator=true
    # node-scheduler.include-coordinator=false
    # http-server.http.port=8080
    # discovery.uri=http://trino-coordinator:8080

  trino-worker:
    image: trinodb/trino:latest
    deploy:
      replicas: 10  # 10 workers across nodes
    configs:
      - source: worker-config
        target: /etc/trino/config.properties
    # config.properties:
    # coordinator=false
    # discovery.uri=http://trino-coordinator:8080
```

**Performance Impact:**
- Query parallelism: 1 coordinator + 10 workers = 10x performance
- Memory: 10 workers × 4GB = 40GB for distributed queries

**Phase 3: Orchestration Layer**

**Option 1: Docker Swarm (Simpler)**
```bash
# Initialize swarm on manager node
docker swarm init --advertise-addr 10.0.0.1

# Join worker nodes
docker swarm join --token <token> 10.0.0.1:2377

# Deploy stack
docker stack deploy -c docker-compose.yml data-platform
```

**Advantages:**
- Native Docker integration
- Simple setup and management
- Built-in service discovery and load balancing

**Disadvantages:**
- Less mature ecosystem than Kubernetes
- Limited advanced scheduling features

**Option 2: Kubernetes (Production-Grade)**
```bash
# 1. Install Kubernetes (kubeadm, k3s, or managed service)
kubeadm init --pod-network-cidr=10.244.0.0/16

# 2. Convert Docker Compose to Kubernetes manifests
kompose convert -f docker-compose.yml

# 3. Deploy with Helm charts (preferred)
helm install kafka bitnami/kafka \
  --set replicaCount=5 \
  --set kraft.enabled=true

helm install spark spark-operator/spark-operator
helm install trino trino/trino
# ... etc for each component

# 4. Apply manifests
kubectl apply -f k8s/
```

**Kubernetes Advantages:**
- Industry standard, mature ecosystem
- Advanced scheduling (node affinity, taints/tolerations)
- Horizontal Pod Autoscaling (HPA)
- StatefulSets for stateful services (Kafka, PostgreSQL)
- Rich monitoring/logging integrations (Prometheus Operator, EFK stack)

**Phase 4: Configuration Changes**

**4.1 Update All Service Endpoints:**

```yaml
# Before (single node):
KAFKA_BOOTSTRAP_SERVERS: kafka1:29092,kafka2:29092,kafka3:29092
S3_ENDPOINT: http://minio:9000
HIVE_METASTORE_URI: thrift://hive-metastore:9083

# After (multi-node with load balancers):
KAFKA_BOOTSTRAP_SERVERS: kafka-lb.data-platform.svc.cluster.local:9092
S3_ENDPOINT: http://minio-lb.data-platform.svc.cluster.local:9000
HIVE_METASTORE_URI: thrift://metastore-lb.data-platform.svc.cluster.local:9083

# Or with external DNS:
KAFKA_BOOTSTRAP_SERVERS: kafka.example.com:9092
S3_ENDPOINT: http://s3.example.com:9000
```

**4.2 Kafka Topic Rebalancing:**

```bash
# Increase partition count for parallelism
kafka-topics.sh --bootstrap-server kafka-lb:9092 \
  --alter --topic banking.transactions.raw \
  --partitions 30  # Up from 3 (10 partitions per worker)

# Reassign partitions across new brokers
kafka-reassign-partitions.sh \
  --bootstrap-server kafka-lb:9092 \
  --reassignment-json-file reassignment.json \
  --execute
```

**4.3 Spark Checkpointing (State Migration):**

```python
# Checkpoint location must be accessible from all workers
CHECKPOINT_LOCATION = "s3a://lakehouse/checkpoints/bronze_transactions/"

# Ensure S3A endpoint is load-balanced MinIO
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio-lb:9000")
```

**Phase 5: Monitoring & Observability Enhancements**

```yaml
# Prometheus Federation (scrape metrics from all nodes)
scrape_configs:
  - job_name: 'kafka-federation'
    honor_labels: true
    metrics_path: '/federate'
    params:
      match[]:
        - '{job="kafka-broker"}'
    static_configs:
      - targets:
        - prometheus-node1:9090
        - prometheus-node2:9090
        # ... all nodes

# Distributed tracing (Jaeger)
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    # Trace Spark jobs, Trino queries end-to-end

# Centralized logging (ELK or Loki)
services:
  loki:
    image: grafana/loki:latest
  promtail:  # Deployed on all nodes
    # Scrapes logs from all containers
```

**Phase 6: Testing & Validation**

1. **Load Testing:**
   ```bash
   # Increase data generation rate
   GENERATION_INTERVAL_MS=100  # 10 records/sec per generator
   # Deploy 10 generator instances = 100 records/sec

   # Monitor Spark processing lag
   # Verify Kafka consumer lag = 0
   ```

2. **Failover Testing:**
   - Kill 1 Kafka broker → Verify cluster operational
   - Kill 1 Spark worker → Verify job continues
   - Kill MinIO node → Verify data accessible (distributed mode)
   - Kill PostgreSQL primary → Verify Patroni failover

3. **Performance Benchmarking:**
   ```sql
   -- Before vs. After query performance
   SELECT COUNT(*) FROM iceberg.bronze.transactions;
   -- Single node: 1s
   -- Multi-node (10 workers): 100ms
   ```

**Estimated Timeline:**
- **Week 1-2:** Infrastructure provisioning, networking
- **Week 3-4:** Kafka and MinIO migration
- **Week 5:** Spark and Trino scaling
- **Week 6:** PostgreSQL HA setup
- **Week 7-8:** Testing, validation, rollback planning
- **Total:** 8 weeks for full migration

**Cost Implications:**
- **Single server:** 1 × $500/month = $500/month
- **Multi-node cluster:** 5 × $800/month = $4,000/month
- **ROI:** 10x performance, high availability, better scalability

**Rollback Plan:**
- Maintain old single-server environment for 2 weeks post-migration
- Use MinIO `mc mirror` to sync data back if needed
- Document all configuration changes for reversal

---

## Monitoring & Observability

### Q4: What metrics should we monitor for Kafka, and how would you set up alerting for critical issues?

**Answer:**

**Critical Kafka Metrics & Alerting Strategy**

**1. Broker Health Metrics**

| Metric | Source | Alert Threshold | Severity | Action |
|--------|--------|-----------------|----------|---------|
| **ActiveControllerCount** | JMX: `kafka.controller:type=KafkaController,name=ActiveControllerCount` | != 1 for > 30s | **Critical** | Check for split-brain or controller election issues |
| **UnderReplicatedPartitions** | JMX: `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | > 0 for > 5 min | **High** | Check broker health, network issues, disk I/O |
| **OfflinePartitionsCount** | JMX: `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` | > 0 | **Critical** | Immediate investigation - data unavailable |
| **IsrShrinksPerSec** | JMX: `kafka.server:type=ReplicaManager,name=IsrShrinksPerSec` | > 0 sustained | **Medium** | Indicates replica lag or broker issues |

**Prometheus Queries:**
```promql
# Alert: No active controller
ALERT NoKafkaController
  IF kafka_controller_kafkacontroller_activecontrollercount != 1
  FOR 30s
  LABELS { severity = "critical" }
  ANNOTATIONS {
    summary = "Kafka cluster has no active controller",
    description = "ActiveControllerCount = {{ $value }}, should be 1"
  }

# Alert: Under-replicated partitions
ALERT UnderReplicatedPartitions
  IF kafka_server_replicamanager_underreplicatedpartitions > 0
  FOR 5m
  LABELS { severity = "high" }
  ANNOTATIONS {
    summary = "Kafka has under-replicated partitions",
    description = "{{ $value }} partitions are under-replicated on {{ $labels.instance }}"
  }

# Alert: Offline partitions (data loss)
ALERT OfflinePartitions
  IF kafka_controller_kafkacontroller_offlinepartitionscount > 0
  FOR 1m
  LABELS { severity = "critical" }
  ANNOTATIONS {
    summary = "Kafka has offline partitions - DATA UNAVAILABLE",
    description = "{{ $value }} partitions are offline"
  }
```

**2. Throughput & Performance Metrics**

| Metric | Source | Alert Threshold | Severity | Action |
|--------|--------|-----------------|----------|---------|
| **BytesInPerSec** | JMX: `kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec` | < 10% of baseline for > 5min | **Medium** | Check producer health, network |
| **BytesOutPerSec** | JMX: `kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec` | < 10% of baseline for > 5min | **Medium** | Check consumer health |
| **RequestHandlerAvgIdlePercent** | JMX: `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` | < 10% | **High** | Broker overloaded, scale up |
| **TotalProduceRequestsPerSec** | JMX: `kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce` | Sudden drop (> 50%) | **Medium** | Producer issues |

**Baseline Calculation:**
```promql
# Calculate 7-day average for BytesInPerSec
avg_over_time(kafka_server_brokertopicmetrics_bytesinpersec[7d])

# Alert if current rate < 10% of 7-day average
ALERT KafkaIngressDrop
  IF kafka_server_brokertopicmetrics_bytesinpersec < 0.1 * avg_over_time(kafka_server_brokertopicmetrics_bytesinpersec[7d])
  FOR 5m
  LABELS { severity = "medium" }
```

**3. Resource Utilization Metrics**

| Metric | Source | Alert Threshold | Severity | Action |
|--------|--------|-----------------|----------|---------|
| **JVM Heap Usage** | JMX: `java.lang:type=Memory,HeapMemoryUsage.used` | > 85% | **High** | Tune heap size or reduce load |
| **GC Pause Time** | JMX: `java.lang:type=GarbageCollector,name=G1 Old Generation,CollectionTime` | > 1s per minute | **High** | GC tuning needed |
| **Disk Usage** | Node Exporter: `node_filesystem_avail_bytes{mountpoint="/var/lib/kafka"}` | < 10% free | **Critical** | Increase retention config |
| **Network Bandwidth** | cAdvisor: `container_network_transmit_bytes_total{name="kafka1"}` | > 800 Mbps (80% of 1Gbps) | **Medium** | Network bottleneck |

**Prometheus Queries:**
```promql
# JVM Heap usage percentage
100 * (jvm_memory_bytes_used{area="heap"} / jvm_memory_bytes_max{area="heap"})

# Alert: High heap usage
ALERT KafkaHighHeapUsage
  IF 100 * (jvm_memory_bytes_used{area="heap",job="kafka-broker"} / jvm_memory_bytes_max{area="heap"}) > 85
  FOR 10m
  LABELS { severity = "high" }
  ANNOTATIONS {
    summary = "Kafka broker {{ $labels.instance }} heap usage is high",
    description = "Heap usage: {{ $value }}%"
  }

# Alert: Disk space low
ALERT KafkaDiskSpaceLow
  IF (node_filesystem_avail_bytes{mountpoint="/var/lib/kafka"} / node_filesystem_size_bytes{mountpoint="/var/lib/kafka"}) < 0.10
  FOR 5m
  LABELS { severity = "critical" }
```

**4. Topic-Specific Metrics**

| Metric | Source | Alert Threshold | Severity | Action |
|--------|--------|-----------------|----------|---------|
| **Message Count (Per Topic)** | JMX: `kafka.log:type=Log,name=Size,topic=banking.transactions.raw` | Growth rate anomaly | **Low** | Informational |
| **Leader Election Rate** | JMX: `kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs` | > 1 per hour | **High** | Cluster instability |
| **Failed Produce Requests** | JMX: `kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec` | > 0 | **Medium** | Check producer configs |

**5. Consumer Lag (Spark Checkpoint-Based)**

Our Spark streaming job uses **checkpoint-based offset management**, not Kafka consumer groups. Standard consumer lag metrics won't show anything. Instead, monitor:

| Metric | Source | Calculation | Alert Threshold |
|--------|--------|-------------|-----------------|
| **Streaming Query Lag** | Spark UI metrics API | `inputRowsPerSecond - processedRowsPerSecond` | Lag > 1000 records |
| **Batch Duration** | Spark UI | Avg batch processing time | > 30s (trigger interval) |
| **Total Delay** | Spark UI | End-to-end latency | > 60s |

**Custom Metric Collection (Spark Streaming):**
```python
# In kafka_to_iceberg_streaming.py
from pyspark.sql.streaming import StreamingQueryListener

class CustomQueryListener(StreamingQueryListener):
    def onQueryProgress(self, event):
        progress = event.progress
        # Push metrics to Prometheus Pushgateway
        push_to_prometheus({
            'spark_streaming_input_rows': progress.numInputRows,
            'spark_streaming_processed_rows': progress.processedRowsPerSecond,
            'spark_streaming_batch_duration_ms': progress.batchDuration,
            'spark_streaming_lag_records': calculate_lag(progress)
        })

spark.streams.addListener(CustomQueryListener())
```

**Prometheus Alert:**
```promql
ALERT SparkStreamingLag
  IF spark_streaming_lag_records > 1000
  FOR 5m
  LABELS { severity = "high" }
  ANNOTATIONS {
    summary = "Spark streaming job is lagging behind Kafka",
    description = "Lag: {{ $value }} records"
  }
```

**6. Alerting Configuration (Alertmanager)**

```yaml
# alertmanager.yml
global:
  resolve_timeout: 5m
  slack_api_url: 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'

route:
  group_by: ['alertname', 'cluster', 'service']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  receiver: 'team-kafka'
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty'
      continue: true
    - match:
        severity: high
      receiver: 'team-kafka'
    - match:
        severity: medium
      receiver: 'slack-warnings'

receivers:
  - name: 'team-kafka'
    slack_configs:
      - channel: '#data-platform-alerts'
        title: 'Kafka Alert: {{ .GroupLabels.alertname }}'
        text: '{{ range .Alerts }}{{ .Annotations.description }}{{ end }}'

  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: 'YOUR_PAGERDUTY_KEY'
        description: '{{ .GroupLabels.alertname }}'

  - name: 'slack-warnings'
    slack_configs:
      - channel: '#data-platform-warnings'
```

**7. Grafana Dashboard Setup**

**Kafka Overview Dashboard Panels:**
1. **Cluster Health:**
   - Active Controller Count (single stat, should be 1)
   - Broker Online Count (gauge, should be 3)
   - Under-Replicated Partitions (single stat, should be 0)
   - Offline Partitions (single stat, should be 0)

2. **Throughput:**
   - Bytes In/Out Per Second (line chart, per broker)
   - Messages Per Second (line chart, per topic)
   - Request Rate (line chart, by request type)

3. **Performance:**
   - Request Handler Idle % (gauge, should be > 20%)
   - Network Processor Idle % (gauge)
   - Average Request Latency (line chart, by request type)

4. **Resource Usage:**
   - JVM Heap Usage % (gauge per broker)
   - GC Pause Time (line chart)
   - Disk Usage (gauge, per broker)
   - Network Bandwidth (line chart)

**Import Dashboard:**
```bash
# Download Kafka dashboard template
curl -o kafka-dashboard.json https://grafana.com/api/dashboards/12460/revisions/1/download

# Import to Grafana
curl -X POST http://admin:admin@localhost:3000/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d @kafka-dashboard.json
```

**8. Monitoring Checklist**

**Daily:**
- [ ] Check Grafana Kafka dashboard for anomalies
- [ ] Review alertmanager for any firing alerts
- [ ] Verify all brokers are online (`make status`)

**Weekly:**
- [ ] Review Kafka logs for ERROR/WARN messages
- [ ] Check JMX metrics trends (heap usage, GC)
- [ ] Verify topic retention is working (disk usage stable)
- [ ] Review Spark streaming lag metrics

**Monthly:**
- [ ] Capacity planning (disk usage growth rate)
- [ ] Performance tuning (GC configuration, heap size)
- [ ] Alert threshold tuning (reduce false positives)

**9. Troubleshooting Runbook**

**Issue: Under-Replicated Partitions**
```bash
# 1. Identify affected partitions
kafka-topics.sh --bootstrap-server kafka1:29092 \
  --describe --under-replicated-partitions

# 2. Check broker health
docker ps | grep kafka
docker logs kafka1 --tail 100

# 3. Check network connectivity
docker exec kafka1 ping kafka2

# 4. Check disk I/O
docker exec kafka1 iostat -x 1 5

# 5. Reassign partitions if needed
kafka-reassign-partitions.sh --execute --reassignment-json-file plan.json
```

**Issue: High Producer Latency**
```bash
# 1. Check producer configuration (acks=all may be slow)
# 2. Monitor network latency between producer and brokers
# 3. Check broker request queue size
# 4. Verify compression is not CPU bottleneck
```

---

### Q5: Our Spark streaming job is processing data in 30-second micro-batches. How would you monitor its health and troubleshoot processing delays?

**Answer:**

**Comprehensive Spark Streaming Monitoring & Troubleshooting Guide**

**1. Key Metrics to Monitor**

**A. Streaming Query Progress Metrics**

The Spark Structured Streaming API exposes detailed metrics via the `StreamingQueryProgress` API:

```python
# Access query metrics in code
query = enriched_df.writeStream...start()

# Get latest progress
progress = query.lastProgress
print(f"Batch ID: {progress['batchId']}")
print(f"Input rows: {progress['numInputRows']}")
print(f"Processing rate: {progress['processedRowsPerSecond']}")
print(f"Batch duration: {progress['batchDuration']} ms")
```

**Critical Metrics:**

| Metric | Source | Healthy Value | Alert Threshold |
|--------|--------|---------------|-----------------|
| **Batch Duration** | `query.lastProgress['durationMs']['total']` | < 30,000 ms | > 30,000 ms (exceeds trigger interval) |
| **Scheduling Delay** | `query.lastProgress['durationMs']['latestOffset']` - previous batch end | < 1,000 ms | > 5,000 ms |
| **Processing Rate** | `query.lastProgress['processedRowsPerSecond']` | > input rate | < input rate (lagging) |
| **Input Rows** | `query.lastProgress['numInputRows']` | ~10,000 (maxOffsetsPerTrigger) | Sudden drop or spike |
| **Watermark Lag** | `query.lastProgress['eventTime']['watermark']` vs. current time | < 60 seconds | > 300 seconds |

**B. Executor Metrics**

| Metric | Source | Healthy Value | Alert Threshold |
|--------|--------|---------------|-----------------|
| **Executor Memory Usage** | Spark UI / Prometheus | < 80% | > 90% |
| **Task Failures** | Spark UI: Stages tab | 0 | > 0 (retry storms) |
| **GC Time** | Spark UI: Executors tab | < 10% of task time | > 30% |
| **Shuffle Read/Write** | Spark UI: Stages tab | Minimal (no shuffle in our job) | Large shuffle indicates data skew |
| **Executor Lost** | Spark UI: Event Timeline | 0 | > 0 |

**C. Kafka Source Metrics**

| Metric | Source | Healthy Value | Alert Threshold |
|--------|--------|---------------|-----------------|
| **Kafka Offset Lag** | `query.lastProgress['sources'][0]['endOffset']` vs. latest | < 10,000 records | > 100,000 records |
| **Kafka Fetch Latency** | Spark UI: SQL tab | < 500 ms | > 2,000 ms |
| **Kafka Partition Reads** | Verify all 3 partitions | Balanced reads | Skewed reads (hot partition) |

**D. Iceberg Write Metrics**

| Metric | Source | Healthy Value | Alert Threshold |
|--------|--------|---------------|-----------------|
| **Write Duration** | `query.lastProgress['durationMs']['addBatch']` | < 20,000 ms | > 25,000 ms |
| **Commit Duration** | Iceberg metadata commit time | < 1,000 ms | > 5,000 ms |
| **Files Written** | Iceberg snapshot metadata | 1-5 files/batch | > 100 files (small file problem) |

---

**2. Monitoring Setup**

**A. Expose Streaming Metrics to Prometheus**

**Method 1: Spark Metrics System (Built-in)**

```python
# spark-defaults.conf
spark.metrics.conf.*.sink.prometheus.class=org.apache.spark.metrics.sink.PrometheusSink
spark.metrics.conf.*.sink.prometheus.pushgateway-address=prometheus-pushgateway:9091
spark.metrics.conf.*.sink.prometheus.period=15
spark.metrics.conf.*.sink.prometheus.unit=seconds
```

**Method 2: Custom Metrics via StreamingQueryListener**

```python
# In kafka_to_iceberg_streaming.py

from pyspark.sql.streaming import StreamingQueryListener
import requests
import time

class PrometheusMetricsListener(StreamingQueryListener):
    def __init__(self, pushgateway_url):
        self.pushgateway_url = pushgateway_url

    def onQueryProgress(self, event):
        progress = event.progress
        timestamp = int(time.time() * 1000)

        metrics = f"""
# HELP spark_streaming_batch_duration_ms Batch processing duration in milliseconds
# TYPE spark_streaming_batch_duration_ms gauge
spark_streaming_batch_duration_ms{{job="bronze_transactions"}} {progress.durationMs.get('triggerExecution', 0)} {timestamp}

# HELP spark_streaming_input_rows Number of input rows in batch
# TYPE spark_streaming_input_rows gauge
spark_streaming_input_rows{{job="bronze_transactions"}} {progress.numInputRows} {timestamp}

# HELP spark_streaming_processed_rate Processing rate (rows/sec)
# TYPE spark_streaming_processed_rate gauge
spark_streaming_processed_rate{{job="bronze_transactions"}} {progress.processedRowsPerSecond or 0} {timestamp}

# HELP spark_streaming_lag_ms End-to-end latency in milliseconds
# TYPE spark_streaming_lag_ms gauge
spark_streaming_lag_ms{{job="bronze_transactions"}} {calculate_lag(progress)} {timestamp}
"""

        # Push to Prometheus Pushgateway
        try:
            response = requests.post(
                f"{self.pushgateway_url}/metrics/job/spark_streaming",
                data=metrics,
                headers={'Content-Type': 'text/plain'}
            )
        except Exception as e:
            print(f"Failed to push metrics: {e}")

    def onQueryStarted(self, event):
        print(f"Query {event.id} started")

    def onQueryTerminated(self, event):
        print(f"Query {event.id} terminated")

def calculate_lag(progress):
    # Calculate lag based on Kafka offsets
    if 'sources' in progress and len(progress['sources']) > 0:
        source = progress['sources'][0]
        start_offset = json.loads(source['startOffset'])
        end_offset = json.loads(source['endOffset'])
        # Lag = latest offset - current offset (requires Kafka API call)
        # Simplified: Use batch duration as proxy
        return progress.durationMs.get('triggerExecution', 0)
    return 0

# Add listener to Spark session
spark.streams.addListener(
    PrometheusMetricsListener("http://prometheus-pushgateway:9091")
)
```

**Deploy Prometheus Pushgateway:**
```yaml
# Add to docker-compose.yml
services:
  prometheus-pushgateway:
    image: prom/pushgateway:latest
    ports:
      - "9091:9091"
    networks:
      - data-platform
```

**Update Prometheus scrape config:**
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'spark-streaming'
    static_configs:
      - targets: ['prometheus-pushgateway:9091']
    honor_labels: true
```

**B. Grafana Dashboard for Streaming Job**

```json
{
  "dashboard": {
    "title": "Spark Streaming - Bronze Transactions",
    "panels": [
      {
        "title": "Batch Duration vs. Trigger Interval",
        "targets": [
          {
            "expr": "spark_streaming_batch_duration_ms{job=\"bronze_transactions\"} / 1000",
            "legendFormat": "Batch Duration (s)"
          },
          {
            "expr": "30",
            "legendFormat": "Trigger Interval (s)"
          }
        ],
        "type": "graph",
        "alert": {
          "conditions": [
            {
              "evaluator": {
                "params": [30000],
                "type": "gt"
              },
              "query": {
                "params": ["A", "5m", "now"]
              }
            }
          ],
          "name": "Batch Duration Exceeds Trigger Interval"
        }
      },
      {
        "title": "Processing Rate vs. Input Rate",
        "targets": [
          {
            "expr": "spark_streaming_processed_rate{job=\"bronze_transactions\"}",
            "legendFormat": "Processed Rows/Sec"
          },
          {
            "expr": "rate(spark_streaming_input_rows{job=\"bronze_transactions\"}[1m]) * 60",
            "legendFormat": "Input Rows/Sec"
          }
        ]
      },
      {
        "title": "End-to-End Latency",
        "targets": [
          {
            "expr": "spark_streaming_lag_ms{job=\"bronze_transactions\"} / 1000",
            "legendFormat": "Latency (s)"
          }
        ]
      },
      {
        "title": "Executor Memory Usage",
        "targets": [
          {
            "expr": "sum(jvm_memory_bytes_used{job=\"spark-worker\",area=\"heap\"}) / sum(jvm_memory_bytes_max{job=\"spark-worker\",area=\"heap\"}) * 100",
            "legendFormat": "Heap Usage %"
          }
        ]
      }
    ]
  }
}
```

---

**3. Troubleshooting Processing Delays**

**Scenario 1: Batch Duration Exceeds 30 Seconds**

**Symptoms:**
- `query.lastProgress['durationMs']['triggerExecution']` > 30,000 ms
- Processing rate < input rate
- Backlog growing in Kafka

**Diagnosis Steps:**

```bash
# 1. Check Spark UI (http://localhost:8888)
# Navigate to: Streaming tab → Statistics

# 2. Identify slow stage
# Look for stages with high "Duration" in Stages tab

# 3. Check executor resource usage
# Executors tab → Look for:
#   - High GC time (> 30% of task time)
#   - Memory pressure (heap usage > 90%)
#   - Task failures/retries

# 4. Analyze slow tasks
# Stages tab → Specific stage → Tasks tab
# Look for:
#   - Task skew (some tasks take 10x longer)
#   - Data skew (partition size imbalance)
#   - Shuffle operations (shouldn't exist in our job)
```

**Root Cause Analysis:**

**A. Iceberg Write Bottleneck (Most Likely)**

```python
# Check Iceberg commit duration
# In foreachBatch function, add timing:

def write_to_iceberg_bronze(batch_df, batch_id):
    start_time = time.time()

    batch_df.writeTo("iceberg.bronze.transactions") \
        .option("write-format", "parquet") \
        .option("compression-codec", "gzip") \
        .append()

    duration = time.time() - start_time
    print(f"Batch {batch_id}: Write duration = {duration:.2f}s")

    # If duration > 25s, investigate:
    # 1. MinIO network latency
    # 2. Hive Metastore latency
    # 3. Small file problem
```

**Solution: Optimize Iceberg Writes**

```python
# Option 1: Reduce compression overhead (GZIP → SNAPPY)
.option("compression-codec", "snappy")  # Faster but larger files

# Option 2: Increase file size (reduce metadata overhead)
.option("write.parquet.row-group-size-bytes", "268435456")  # 256 MB

# Option 3: Disable compaction during write (faster writes, manual compaction later)
.option("write.metadata.metrics.default", "truncate(16)")  # Reduce metadata size
```

**B. Memory Pressure / GC Overhead**

```bash
# Check executor memory usage
docker exec spark-worker-1 jstat -gc <PID> 1000

# If Old Gen is full or GC time > 30%:
# Solution: Increase executor memory or reduce batch size
```

**Fix in `docker-compose.yml`:**
```yaml
spark-worker-1:
  environment:
    SPARK_WORKER_MEMORY: 8G  # Increase from 4G
```

**Or reduce batch size:**
```python
# In kafka_to_iceberg_streaming.py
.option("maxOffsetsPerTrigger", "5000")  # Reduce from 10,000
```

**C. Kafka Source Slow (Network/Deserialization)**

```python
# Check Kafka fetch duration in Spark UI
# SQL tab → Expand query → Look for "Scan Kafka" operation

# If > 2 seconds, optimize:
# 1. Increase fetch buffer size
.option("kafka.fetch.max.bytes", "104857600")  # 100 MB
.option("kafka.max.partition.fetch.bytes", "10485760")  # 10 MB

# 2. Increase max poll records
.option("kafka.max.poll.records", "10000")

# 3. Use multiple Kafka readers (automatic with 3 partitions)
# Verify: Spark creates 1 task per Kafka partition
```

---

**Scenario 2: Processing Rate < Input Rate (Lagging Behind)**

**Symptoms:**
- Kafka offset lag increasing over time
- `processedRowsPerSecond` < `inputRowsPerSecond`
- Batch duration approaching or exceeding 30 seconds

**Solutions:**

**A. Add More Executors (Scale Workers)**

```yaml
# docker-compose.yml
services:
  spark-worker-3:  # Add 3rd worker
    image: bitnami/spark:3.5.0
    environment:
      SPARK_MODE: worker
      SPARK_WORKER_CORES: 2
      SPARK_WORKER_MEMORY: 4G
      SPARK_MASTER_URL: spark://spark-master:7077
```

**B. Increase Executor Resources**

```bash
# Resubmit job with more resources
spark-submit \
  --executor-memory 6g \    # Up from 3g
  --executor-cores 3 \      # Up from 2
  --total-executor-cores 6  # 2 workers × 3 cores
  # ... rest of command
```

**C. Optimize Spark Configuration**

```python
# In kafka_to_iceberg_streaming.py

# 1. Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 2. Optimize shuffle (not applicable to our job, but good practice)
spark.conf.set("spark.sql.shuffle.partitions", "6")  # Match executor cores

# 3. Tune serialization
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.unsafe", "true")
```

---

**Scenario 3: Intermittent Task Failures**

**Symptoms:**
- Tasks failing with `FetchFailedException` or `ExecutorLostFailure`
- Spark retrying tasks multiple times
- Batch duration spikes due to retries

**Diagnosis:**

```bash
# Check executor logs
docker logs spark-worker-1 --tail 500 | grep -i error

# Common errors:
# - "OutOfMemoryError: Java heap space" → Increase executor memory
# - "Connection refused to MinIO" → Network/DNS issue
# - "NoSuchMethodError" → Dependency version mismatch
```

**Solutions:**

**A. Network Issues (MinIO Connection)**

```bash
# Test connectivity from Spark worker
docker exec spark-worker-1 curl -I http://minio:9000/minio/health/live

# If fails, check:
# 1. DNS resolution
docker exec spark-worker-1 nslookup minio

# 2. Network connectivity
docker exec spark-worker-1 ping minio

# 3. MinIO health
docker logs minio
```

**B. Dependency Conflicts**

```python
# Ensure compatible versions in spark-submit
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,\
           org.apache.hadoop:hadoop-aws:3.3.4,\  # Must match Spark's Hadoop version
           org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0  # Must match Spark version
```

**C. Executor Timeout**

```python
# Increase timeout for long-running tasks
spark.conf.set("spark.network.timeout", "600s")  # Default: 120s
spark.conf.set("spark.executor.heartbeatInterval", "60s")  # Default: 10s
```

---

**Scenario 4: Checkpoint Corruption**

**Symptoms:**
- Job fails to start with `CheckpointFileManager` error
- Offset metadata corrupted in S3

**Recovery:**

```bash
# 1. Check checkpoint location
aws s3 ls s3://lakehouse/checkpoints/bronze_transactions/ \
  --recursive --endpoint-url http://localhost:9000

# 2. If corrupted, delete and restart (WARNING: May reprocess data)
aws s3 rm s3://lakehouse/checkpoints/bronze_transactions/ \
  --recursive --endpoint-url http://localhost:9000

# 3. Restart job (will start from "earliest" offset)
make restart-streaming

# 4. To avoid reprocessing, restore from backup
aws s3 sync s3://backups/checkpoints/bronze_transactions/ \
  s3://lakehouse/checkpoints/bronze_transactions/ \
  --endpoint-url http://localhost:9000
```

**Prevention:**
```bash
# Regular checkpoint backups
0 */6 * * * aws s3 sync s3://lakehouse/checkpoints/ s3://backups/checkpoints/ --endpoint-url http://localhost:9000
```

---

**4. Proactive Monitoring Checklist**

**Hourly (Automated Alerts):**
- [ ] Batch duration < 30 seconds
- [ ] No task failures in last hour
- [ ] Kafka offset lag < 10,000 records

**Daily (Manual Review):**
- [ ] Review Spark UI: Check for task skew, GC overhead
- [ ] Check Grafana dashboard: Look for trends
- [ ] Verify checkpoint integrity: `aws s3 ls ...`

**Weekly:**
- [ ] Analyze slow batches: Identify bottlenecks
- [ ] Review executor logs: Check for warnings
- [ ] Capacity planning: Project lag growth

**Monthly:**
- [ ] Performance tuning: Adjust configs based on metrics
- [ ] Checkpoint cleanup: Remove old checkpoints
- [ ] Backup validation: Test restore procedures

---

