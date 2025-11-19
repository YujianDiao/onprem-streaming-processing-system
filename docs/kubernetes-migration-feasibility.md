# Kubernetes Migration Feasibility Analysis
## On-Prem Data Lakehouse to AWS EKS

**Date:** 2025-11-19
**Current Setup:** Single-node Docker Compose (25+ services)
**Target:** Kubernetes on AWS EC2 with limited resources (8 cores, 16-32GB RAM per node)

---

## Executive Summary

**Verdict: FEASIBLE with strategic optimizations**

Migrating the current on-prem data lakehouse to Kubernetes on EC2 instances with limited resources (8 cores, 16-32GB RAM) is **feasible** but requires careful architecture adjustments. The current setup runs 25+ services on a single node, consuming approximately 24-32GB RAM and 16+ cores for full operation. To successfully migrate to resource-constrained EC2 instances, we recommend:

- **Minimum 4 EC2 instances** (t3.xlarge or t3a.xlarge: 4 vCPUs, 16GB RAM each)
- **Recommended 5-6 EC2 instances** for production-grade reliability with high availability
- **Strategic service consolidation** and resource optimization
- **Stateful service migration** to managed AWS services where cost-effective

**Total Cluster Resources:**
- **Minimum Config:** 4 nodes × 4 cores × 16GB = 16 cores, 64GB RAM (usable: ~14 cores, 52GB after K8s overhead)
- **Recommended Config:** 6 nodes × 4 cores × 16GB = 24 cores, 96GB RAM (usable: ~21 cores, 78GB after K8s overhead)

---

## Current Architecture Resource Analysis

### Resource Requirements by Component Layer

#### **1. Kafka Cluster (Messaging Layer)**
| Component | CPU | Memory | Storage | Replicas |
|-----------|-----|--------|---------|----------|
| kafka-1 | 1-2 cores | 1-2GB | 20GB | 1 |
| kafka-2 | 1-2 cores | 1-2GB | 20GB | 1 |
| kafka-3 | 1-2 cores | 1-2GB | 20GB | 1 |
| schema-registry | 0.5 cores | 512MB | 1GB | 1 |
| kafka-ui | 0.25 cores | 256MB | 1GB | 1 |
| **Subtotal** | **5-8 cores** | **5-8GB** | **63GB** | **5** |

**Configuration:** 3-broker KRaft cluster, replication factor 2

#### **2. Storage Layer**
| Component | CPU | Memory | Storage | Replicas |
|-----------|-----|--------|---------|----------|
| MinIO | 1-2 cores | 2-4GB | 200GB+ | 1-4 |
| PostgreSQL | 0.5-1 cores | 512MB-1GB | 10GB | 1 |
| Hive Metastore | 1 core | 1-2GB | 5GB | 1 |
| **Subtotal** | **2.5-5 cores** | **4-7GB** | **215GB** | **3-6** |

#### **3. Processing Layer (Apache Spark)**
| Component | CPU | Memory | Storage | Replicas |
|-----------|-----|--------|---------|----------|
| spark-master | 1 core | 1GB | 5GB | 1 |
| spark-worker-1 | 2 cores | 4GB | 10GB | 1 |
| spark-worker-2 | 2 cores | 4GB | 10GB | 1 |
| **Subtotal** | **5 cores** | **9GB** | **25GB** | **3** |

#### **4. Analytics Layer**
| Component | CPU | Memory | Storage | Replicas |
|-----------|-----|--------|---------|----------|
| Trino | 2 cores | 3-4GB | 10GB | 1-2 |
| **Subtotal** | **2 cores** | **3-4GB** | **10GB** | **1-2** |

#### **5. Monitoring & Observability**
| Component | CPU | Memory | Storage | Replicas |
|-----------|-----|--------|---------|----------|
| Prometheus | 0.5 cores | 1-2GB | 20GB | 1 |
| Grafana | 0.25 cores | 256MB | 2GB | 1 |
| cAdvisor | 0.25 cores | 256MB | 1GB | Per node |
| node-exporter | 0.1 cores | 128MB | 1GB | Per node |
| postgres-exporter | 0.1 cores | 128MB | 1GB | 1 |
| jmx-exporters (3) | 0.3 cores | 384MB | 3GB | 3 |
| **Subtotal** | **1.5 cores** | **2-3GB** | **28GB** | **7+** |

#### **6. Visualization (Optional)**
| Component | CPU | Memory | Storage | Replicas |
|-----------|-----|--------|---------|----------|
| Superset | 1 core | 1-2GB | 5GB | 1 |
| **Subtotal** | **1 core** | **1-2GB** | **5GB** | **1** |

#### **7. Data Generation**
| Component | CPU | Memory | Storage | Replicas |
|-----------|-----|--------|---------|----------|
| data-generator | 0.5 cores | 512MB | 1GB | 1 |
| **Subtotal** | **0.5 cores** | **512MB** | **1GB** | **1** |

---

### **TOTAL CURRENT RESOURCE REQUIREMENTS**

| Layer | CPU | Memory | Storage |
|-------|-----|--------|---------|
| **Core Services (Required)** | **15-20 cores** | **22-30GB** | **314GB** |
| **Full Stack (with optional)** | **17-22 cores** | **24-32GB** | **346GB** |

**Current Deployment:** Single node with 16-32GB RAM, 16+ cores, 200GB+ SSD

---

## Kubernetes Migration Challenges

### 1. **Kubernetes Overhead**
- **Control plane components:** 1-2GB RAM, 1-2 cores per master
- **kube-proxy, kubelet, CNI:** ~200-400MB per node
- **Effective capacity:** ~80-85% of total resources usable

### 2. **Stateful Workloads**
The following services require persistent volumes with proper backup/replication:
- **Kafka brokers:** 3 × 20GB (logs, metadata)
- **MinIO:** 200GB+ (lakehouse data)
- **PostgreSQL:** 10GB (Hive metastore)
- **Prometheus:** 20GB (metrics retention)
- **Grafana:** 2GB (dashboards)

**Solution:** Use EBS volumes with `gp3` type, snapshots enabled

### 3. **Inter-Service Network Latency**
- Single-node: localhost communication (~0.01ms)
- Kubernetes: Pod-to-pod across nodes (~0.5-2ms)
- **Impact:** Minimal for lakehouse architecture (batch-oriented)

### 4. **High Availability Requirements**
Current setup has NO redundancy. Kubernetes migration offers opportunity for:
- Multiple Kafka brokers across nodes
- Replicated Spark workers
- MinIO distributed mode (4+ nodes)
- Trino worker pools

---

## EC2 Instance Recommendations

### **Instance Type Analysis**

| Instance Type | vCPU | Memory | Network | Price/hr (us-east-1) | Use Case |
|---------------|------|--------|---------|----------------------|----------|
| **t3.xlarge** | 4 | 16GB | Up to 5 Gbps | $0.1664 | Recommended |
| **t3a.xlarge** | 4 | 16GB | Up to 5 Gbps | $0.1498 | Cost-optimized |
| **t3.2xlarge** | 8 | 32GB | Up to 5 Gbps | $0.3328 | Alternative (fewer nodes) |
| **m5.xlarge** | 4 | 16GB | Up to 10 Gbps | $0.192 | High network |
| **m5a.xlarge** | 4 | 16GB | Up to 10 Gbps | $0.172 | Balanced |
| **r5.xlarge** | 4 | 32GB | Up to 10 Gbps | $0.252 | Memory-intensive |

**Recommended:** `t3.xlarge` or `t3a.xlarge` (AMD, 10% cheaper)

---

## Kubernetes Cluster Architecture Options

### **Option 1: Minimum Viable Cluster (4 nodes)**

**Configuration:**
- **1 Control Plane node:** t3.xlarge (4 vCPU, 16GB RAM)
- **3 Worker nodes:** t3.xlarge (4 vCPU, 16GB RAM each)

**Total Resources:**
- **Cores:** 16 total (usable: ~14 cores after overhead)
- **Memory:** 64GB total (usable: ~52GB after overhead)
- **Monthly Cost:** ~$480/month (on-demand)

**Service Distribution:**
```
Node 1 (Control Plane + Worker):
  - Kubernetes control plane
  - Kafka-1, Schema Registry
  - PostgreSQL, Hive Metastore
  - Prometheus, Grafana

Node 2 (Worker):
  - Kafka-2
  - MinIO (single instance)
  - Trino
  - Data Generator

Node 3 (Worker):
  - Kafka-3
  - Spark Master + Worker-1
  - Superset
  - Monitoring exporters

Node 4 (Worker):
  - Spark Worker-2
  - Kafka UI
  - Additional monitoring
```

**Pros:**
- Minimum cost
- Meets resource requirements
- Basic fault tolerance

**Cons:**
- Control plane on worker (not ideal for production)
- Limited headroom for scaling
- Single point of failure for some services

---

### **Option 2: Recommended Production Cluster (6 nodes)**

**Configuration:**
- **3 Control Plane nodes:** t3.medium (2 vCPU, 4GB RAM each) - HA control plane
- **3 Worker nodes:** t3.xlarge (4 vCPU, 16GB RAM each)

**OR (cost-optimized):**
- **1 Control Plane node:** t3.large (2 vCPU, 8GB RAM)
- **5 Worker nodes:** t3.xlarge (4 vCPU, 16GB RAM each)

**Total Resources (Option 2b):**
- **Cores:** 22 total (usable: ~20 cores)
- **Memory:** 88GB total (usable: ~72GB)
- **Monthly Cost:** ~$720/month (on-demand)

**Service Distribution:**
```
Node 1 (Control Plane):
  - Kubernetes control plane only

Node 2 (Worker - Kafka):
  - Kafka-1
  - Kafka-2
  - Schema Registry

Node 3 (Worker - Kafka):
  - Kafka-3
  - Kafka UI
  - JMX Exporters

Node 4 (Worker - Storage):
  - MinIO (or use S3)
  - PostgreSQL
  - Hive Metastore

Node 5 (Worker - Processing):
  - Spark Master
  - Spark Worker-1
  - Spark Worker-2

Node 6 (Worker - Analytics/Monitoring):
  - Trino
  - Prometheus
  - Grafana
  - Superset
  - Data Generator
```

**Pros:**
- Production-ready with HA
- Clear service separation
- Room for scaling (20-30% headroom)
- Better isolation and fault tolerance

**Cons:**
- Higher cost (~$720/month)

---

### **Option 3: Hybrid with Managed Services (4-5 nodes)**

**Configuration:**
- **1 Control Plane:** t3.large (2 vCPU, 8GB RAM)
- **3-4 Worker nodes:** t3.xlarge (4 vCPU, 16GB RAM each)
- **Managed Services:**
  - Amazon MSK (Managed Kafka) - $0.21/broker-hour
  - Amazon RDS PostgreSQL - db.t3.small ($0.034/hr)
  - Amazon S3 (replace MinIO) - $0.023/GB

**Total Monthly Cost:**
- EKS nodes: $480-600
- MSK (3 brokers): ~$450
- RDS: ~$25
- S3: ~$5-10
- **Total: ~$960-1085/month**

**Pros:**
- Managed Kafka reduces operational burden
- RDS provides automated backups, HA
- S3 is industry-standard, highly durable
- Fewer services to manage in K8s

**Cons:**
- Higher total cost
- Vendor lock-in
- Less control over Kafka configuration

---

## Recommended Architecture: **Option 2b (5-6 nodes)**

**Rationale:**
1. **Resource fit:** 20 usable cores, 72GB RAM matches requirements
2. **High availability:** Services can be replicated across nodes
3. **Cost-effective:** $720/month is reasonable for production lakehouse
4. **Scalability:** 20-30% headroom for growth
5. **Full control:** All services self-hosted, no vendor lock-in

---

## Migration Strategy & Implementation Plan

### **Phase 1: Pre-Migration (Week 1-2)**

#### 1.1 Infrastructure Setup
- [ ] Provision EKS cluster (1.28+) in AWS
- [ ] Create 5-6 EC2 worker nodes (t3.xlarge)
- [ ] Configure EBS CSI driver for persistent volumes
- [ ] Set up VPC, subnets, security groups
- [ ] Configure IAM roles (IRSA for S3 access)

#### 1.2 Kubernetes Components
- [ ] Install Helm 3
- [ ] Deploy ingress controller (NGINX or ALB Ingress Controller)
- [ ] Install cert-manager for TLS
- [ ] Deploy metrics-server
- [ ] Configure storage classes (gp3, fast SSD)

#### 1.3 Monitoring Stack (Deploy First)
- [ ] Deploy Prometheus Operator
- [ ] Configure Grafana with persistent storage
- [ ] Set up ServiceMonitors for all services
- [ ] Create initial dashboards

---

### **Phase 2: Stateless Service Migration (Week 2-3)**

#### 2.1 Core Infrastructure Services
Deploy in order:

1. **Schema Registry**
   - Deploy as Deployment (1 replica initially)
   - ConfigMap for configuration
   - Service (ClusterIP)

2. **Kafka UI**
   - Lightweight deployment
   - Ingress for external access

3. **Data Generator**
   - Deployment (1 replica)
   - ConfigMap for Kafka connection

#### 2.2 Analytics Services

4. **Trino**
   - Deployment (coordinator + workers)
   - ConfigMap for catalog configuration
   - Service (LoadBalancer or Ingress)

5. **Superset**
   - Deployment with PVC for metadata DB
   - Ingress for web UI

---

### **Phase 3: Stateful Service Migration (Week 3-4)**

#### 3.1 Databases & Metadata

1. **PostgreSQL (Hive Metastore Backend)**
   - StatefulSet with 1 replica
   - PVC: 20GB gp3 EBS
   - Service (ClusterIP)
   - **Migration:** pg_dump from old system, pg_restore to new

2. **Hive Metastore**
   - Deployment (1-2 replicas)
   - Connects to PostgreSQL
   - Service (ClusterIP on port 9083)

#### 3.2 Object Storage

3. **MinIO (or migrate to S3)**

   **Option A: MinIO on Kubernetes**
   - StatefulSet with 4 replicas (distributed mode)
   - 4 × 100GB PVCs (gp3)
   - Service (LoadBalancer)
   - **Migration:** `mc mirror` from old MinIO to new

   **Option B: Amazon S3**
   - Create S3 bucket
   - Configure IRSA for pod access
   - **Migration:** `aws s3 sync` or `mc mirror`
   - Update Spark/Trino configs to use S3

---

### **Phase 4: Kafka Cluster Migration (Week 4-5)**

#### 4.1 Kafka Deployment Options

**Option A: Strimzi Operator (Recommended)**
```bash
# Install Strimzi operator
kubectl create namespace kafka
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka'

# Deploy Kafka cluster with KRaft
kubectl apply -f kafka-cluster.yaml
```

**Configuration:**
- 3 Kafka brokers (StatefulSet)
- KRaft mode (no Zookeeper)
- Pod anti-affinity (spread across nodes)
- 3 × 20GB PVCs for each broker
- Replication factor: 2

**Option B: Confluent for Kubernetes**
- Enterprise features
- Schema Registry integration
- More complex setup

#### 4.2 Migration Steps

1. **Deploy new Kafka cluster** on Kubernetes
2. **Create topics** with same configuration
3. **MirrorMaker 2** to replicate data from old to new cluster
4. **Switch producers** (data-generator) to new cluster
5. **Switch consumers** (Spark) to new cluster
6. **Validate** data consistency
7. **Decommission** old Kafka cluster

**Topic Migration:**
```bash
# Export topic configs from old cluster
kafka-topics.sh --bootstrap-server OLD_KAFKA:9092 --describe --topic banking.transactions.raw > topic-config.txt

# Create topics on new cluster
kafka-topics.sh --bootstrap-server NEW_KAFKA:9092 --create --topic banking.transactions.raw \
  --partitions 3 --replication-factor 2
```

---

### **Phase 5: Spark Cluster Migration (Week 5-6)**

#### 5.1 Spark on Kubernetes Options

**Option A: Spark Operator (Recommended)**
```bash
# Install Spark Operator
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install spark-operator spark-operator/spark-operator --namespace spark-operator --create-namespace
```

**Option B: Standalone Spark on K8s**
- Deploy Spark Master as Deployment
- Deploy Spark Workers as StatefulSet
- Use same architecture as Docker Compose

#### 5.2 Deployment Configuration

```yaml
# SparkApplication CRD for streaming job
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: kafka-to-iceberg-streaming
spec:
  type: Python
  mode: cluster
  image: apache/spark:3.5.0
  mainApplicationFile: s3a://lakehouse/jobs/kafka_to_iceberg_streaming.py
  sparkConf:
    spark.executor.instances: "2"
    spark.executor.cores: "2"
    spark.executor.memory: "3g"
    spark.driver.memory: "2g"
  driver:
    serviceAccount: spark
  executor:
    serviceAccount: spark
```

#### 5.3 Migration Steps

1. **Deploy Spark Operator** or Standalone cluster
2. **Upload Spark jobs** to S3/MinIO
3. **Configure RBAC** for Spark service account
4. **Test batch job** first (batch_aggregations.py)
5. **Deploy streaming job** with checkpoint recovery
6. **Monitor** for 24-48 hours
7. **Decommission** old Spark cluster

**Checkpoint Migration:**
- Copy S3 checkpoints to new MinIO/S3: `s3a://lakehouse/checkpoints/`
- Spark Structured Streaming will resume from last offset

---

### **Phase 6: Monitoring & Observability (Week 6)**

#### 6.1 Prometheus Stack

Deploy using kube-prometheus-stack:
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install kube-prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace
```

**Components:**
- Prometheus Operator
- Grafana
- Alertmanager
- Node Exporter (DaemonSet)
- kube-state-metrics

#### 6.2 Service-Specific Exporters

1. **Kafka Exporter**
   - Deploy as Deployment
   - ServiceMonitor for Prometheus

2. **PostgreSQL Exporter**
   - Sidecar container in PostgreSQL pod

3. **JMX Exporters for Kafka**
   - Sidecar containers in Kafka pods

#### 6.3 Dashboards

- Import Kafka dashboards
- Import Spark dashboards
- Create custom Iceberg/Lakehouse dashboards
- Set up alerts for critical services

---

### **Phase 7: Validation & Cutover (Week 7)**

#### 7.1 Pre-Cutover Checks

- [ ] All services running and healthy
- [ ] Kafka topics replicated and in-sync
- [ ] Spark streaming job processing with <30s latency
- [ ] Trino queries returning correct results
- [ ] Monitoring dashboards operational
- [ ] Backup/restore tested for all stateful services

#### 7.2 Cutover Process

1. **Stop data generator** on old system
2. **Wait for Spark to catch up** (process all Kafka messages)
3. **Verify data consistency** (row counts, checksums)
4. **Switch DNS/ingress** to new cluster
5. **Start data generator** on new system
6. **Monitor for 48 hours**
7. **Decommission old system** after successful validation

#### 7.3 Rollback Plan

- Keep old system running for 1-2 weeks
- MirrorMaker can reverse-replicate if needed
- Database backups available
- DNS switch back to old system

---

### **Phase 8: Optimization & Scaling (Week 8+)**

#### 8.1 Resource Optimization

- Right-size pod requests/limits based on actual usage
- Enable Horizontal Pod Autoscaler (HPA) for:
  - Trino workers (based on CPU)
  - Spark workers (based on queue length)
  - Superset (based on requests)

#### 8.2 Cost Optimization

- Evaluate Spot Instances for worker nodes (50-70% savings)
- Use Reserved Instances or Savings Plans (40% savings)
- Implement cluster autoscaling with Karpenter or Cluster Autoscaler
- Schedule non-critical workloads during off-peak hours

#### 8.3 High Availability Improvements

- Multi-AZ deployment for stateful services
- PodDisruptionBudgets for all critical services
- Automated backups with Velero
- Disaster recovery testing

---

## Kubernetes Manifests Structure

```
k8s/
├── namespaces/
│   ├── kafka.yaml
│   ├── spark.yaml
│   ├── storage.yaml
│   ├── analytics.yaml
│   └── monitoring.yaml
├── storage/
│   ├── storage-classes.yaml
│   └── pvcs/
│       ├── postgres-pvc.yaml
│       ├── minio-pvc.yaml
│       └── prometheus-pvc.yaml
├── kafka/
│   ├── strimzi-operator.yaml
│   ├── kafka-cluster.yaml
│   ├── topics.yaml
│   └── schema-registry.yaml
├── storage-layer/
│   ├── postgres.yaml
│   ├── hive-metastore.yaml
│   └── minio.yaml (or s3-config.yaml)
├── spark/
│   ├── spark-operator.yaml
│   ├── spark-master.yaml
│   ├── spark-workers.yaml
│   ├── streaming-job.yaml
│   └── batch-job.yaml
├── analytics/
│   ├── trino.yaml
│   └── superset.yaml
├── monitoring/
│   ├── prometheus-stack.yaml
│   ├── servicemonitors/
│   │   ├── kafka-monitor.yaml
│   │   ├── postgres-monitor.yaml
│   │   └── spark-monitor.yaml
│   └── dashboards/
│       └── lakehouse-dashboard.yaml
├── ingress/
│   ├── kafka-ui-ingress.yaml
│   ├── trino-ingress.yaml
│   ├── superset-ingress.yaml
│   └── grafana-ingress.yaml
└── data-generator/
    └── data-generator.yaml
```

---

## Resource Allocation Strategy for 5-6 Node Cluster

### **Node Labeling & Taints**

```yaml
# Node 1: Control Plane
node-role.kubernetes.io/control-plane: ""
taints: node-role.kubernetes.io/control-plane:NoSchedule

# Node 2: Kafka
workload-type: kafka

# Node 3: Kafka
workload-type: kafka

# Node 4: Storage
workload-type: storage

# Node 5: Processing
workload-type: processing

# Node 6: Analytics
workload-type: analytics
```

### **Pod Resource Requests & Limits**

```yaml
# Kafka Broker
resources:
  requests:
    cpu: 1000m
    memory: 2Gi
  limits:
    cpu: 2000m
    memory: 3Gi

# Spark Worker
resources:
  requests:
    cpu: 2000m
    memory: 4Gi
  limits:
    cpu: 2000m
    memory: 5Gi

# Trino
resources:
  requests:
    cpu: 1500m
    memory: 3Gi
  limits:
    cpu: 2500m
    memory: 4Gi

# PostgreSQL
resources:
  requests:
    cpu: 500m
    memory: 1Gi
  limits:
    cpu: 1000m
    memory: 2Gi

# MinIO
resources:
  requests:
    cpu: 1000m
    memory: 2Gi
  limits:
    cpu: 2000m
    memory: 4Gi
```

---

## Cost Breakdown (Monthly)

### **Option 2b: 5 Worker Nodes + 1 Control Plane**

| Component | Instance | Quantity | Cost/hr | Monthly |
|-----------|----------|----------|---------|---------|
| Control Plane | t3.large | 1 | $0.0832 | $60 |
| Worker Nodes | t3.xlarge | 5 | $0.1664 | $600 |
| EBS Volumes (gp3) | 100GB | 10 | - | $80 |
| Network Transfer | - | 1TB/month | - | $50 |
| **Total (On-Demand)** | | | | **$790** |
| **With 1yr Reserved Instances** | | | | **$490** |
| **With 3yr Reserved Instances** | | | | **$320** |

**Cost Comparison:**
- Current on-prem: Server depreciation, power, network (~$200-400/month)
- AWS EKS (on-demand): $790/month
- AWS EKS (1yr RI): $490/month
- AWS EKS (3yr RI): $320/month

**ROI:** With 1-year reserved instances, break-even at ~6-8 months vs. maintaining on-prem hardware.

---

## Risk Assessment & Mitigation

### **High Risks**

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Data loss during migration | Critical | Low | Full backups before migration, MirrorMaker for Kafka, parallel run for 1 week |
| Kafka offset loss | High | Medium | Use checkpoint-based offset management, test recovery |
| Insufficient resources | High | Medium | Monitor closely during migration, keep 20% headroom, scale up if needed |
| Network latency impact | Medium | Low | Test inter-pod latency, consider placement groups |

### **Medium Risks**

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Cost overrun | Medium | Medium | Use Reserved Instances, monitor with Cost Explorer, set billing alerts |
| Spark job failure | Medium | Medium | Implement checkpointing, test recovery, have rollback plan |
| Learning curve | Medium | High | Training, documentation, phased migration |

### **Low Risks**

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Service incompatibility | Low | Low | Test all services in staging first |
| Performance degradation | Low | Low | Load testing before cutover |

---

## Success Metrics

### **Technical KPIs**

- [ ] All services migrated and running
- [ ] Kafka lag < 1000 messages
- [ ] Spark streaming latency < 60 seconds
- [ ] Trino query performance within 10% of baseline
- [ ] System uptime > 99.5%
- [ ] Zero data loss

### **Operational KPIs**

- [ ] Migration completed within 8 weeks
- [ ] Less than 4 hours downtime during cutover
- [ ] All team members trained on Kubernetes
- [ ] Documentation complete
- [ ] Monitoring dashboards operational

### **Business KPIs**

- [ ] Cost within budget ($800/month)
- [ ] Ability to scale to 2x current load
- [ ] Improved disaster recovery (RTO < 1 hour)
- [ ] Reduced operational burden (managed K8s)

---

## Timeline Summary

| Phase | Duration | Key Deliverables |
|-------|----------|------------------|
| 1. Pre-Migration | 2 weeks | EKS cluster, monitoring, storage |
| 2. Stateless Services | 1 week | Trino, Superset, UI components |
| 3. Stateful Services | 1 week | PostgreSQL, Hive, MinIO |
| 4. Kafka Migration | 1 week | Kafka cluster, topic replication |
| 5. Spark Migration | 1 week | Spark cluster, streaming jobs |
| 6. Monitoring | 1 week | Full observability stack |
| 7. Validation & Cutover | 1 week | Testing, cutover, validation |
| 8. Optimization | Ongoing | Cost optimization, scaling |

**Total:** 7-8 weeks for full migration

---

## Conclusion

Migrating the on-prem data lakehouse to Kubernetes on EC2 instances with limited resources is **feasible and recommended** with the following configuration:

- **5-6 EC2 instances** (t3.xlarge: 4 cores, 16GB RAM)
- **Total cluster resources:** 20-24 cores, 72-88GB RAM
- **Estimated cost:** $490-790/month (depending on RI commitment)
- **Migration timeline:** 7-8 weeks
- **Risk level:** Medium (mitigated with proper planning)

**Next Steps:**
1. Approve budget and architecture
2. Provision EKS cluster
3. Begin Phase 1 infrastructure setup
4. Follow phased migration plan
5. Validate and optimize

**Alternative:** If cost is a concern, consider the hybrid approach (Option 3) using Amazon MSK and RDS, which reduces operational complexity but increases total cost to ~$960-1085/month.
