# Kubernetes Migration Quick Reference

**Quick answers to your questions about migrating the on-prem data lakehouse to Kubernetes**

---

## Is it Feasible?

**✅ YES** - Migrating to Kubernetes on EC2 instances with 8 cores and 16-32GB RAM per node is **feasible and recommended**.

---

## How Many EC2 Instances?

### **Minimum Configuration (Cost-Optimized)**
- **4 EC2 instances** (t3.xlarge: 4 vCPUs, 16GB RAM each)
- Total: 16 cores, 64GB RAM
- Monthly cost: ~$480 (on-demand), ~$290 (1yr RI)
- Use case: Development/staging environments

### **Recommended Configuration (Production)**
- **5-6 EC2 instances** (t3.xlarge: 4 vCPUs, 16GB RAM each)
- Total: 20-24 cores, 80-96GB RAM
- Monthly cost: ~$600-720 (on-demand), ~$360-432 (1yr RI)
- Use case: Production with high availability

### **Instance Type Recommendation**
- **Primary choice:** `t3.xlarge` or `t3a.xlarge` (AMD, 10% cheaper)
- **Alternative:** `m5a.xlarge` for better network performance
- **Memory-intensive workloads:** `r5.xlarge` (32GB RAM)

---

## Current vs. Kubernetes Resource Requirements

| Metric | Current (Single Node) | Kubernetes (5 nodes) |
|--------|----------------------|---------------------|
| **Services** | 25+ containers | 25+ pods |
| **Total CPU** | 16-20 cores | 20 cores (usable) |
| **Total Memory** | 24-32GB | 64-80GB (usable) |
| **Storage** | 200GB+ local | 300GB+ EBS |
| **Availability** | Single point of failure | Multi-node, HA |
| **Scalability** | Limited | Horizontal scaling |

---

## Architecture Comparison

### **Current On-Prem Setup**
```
Single Node (Docker Compose)
├── 3 Kafka brokers
├── 2 Spark workers
├── 1 Trino coordinator
├── PostgreSQL
├── MinIO
├── Hive Metastore
├── Monitoring stack (Prometheus, Grafana)
└── Visualization (Superset)

Limitations:
- No redundancy
- Limited scaling
- Manual recovery
```

### **Kubernetes Setup (Recommended)**
```
5-Node EKS Cluster

Node 1: Control Plane
├── Kubernetes API server
├── etcd
└── Controller manager

Node 2-3: Kafka Layer
├── Kafka-1, Kafka-2, Kafka-3
├── Schema Registry
└── Kafka UI

Node 4: Storage Layer
├── PostgreSQL (StatefulSet)
├── Hive Metastore
└── MinIO (or use S3)

Node 5: Processing Layer
├── Spark Master
├── Spark Workers (auto-scaling)
└── Data Generator

Node 6: Analytics Layer
├── Trino
├── Superset
├── Prometheus
└── Grafana

Benefits:
✅ High availability
✅ Auto-scaling
✅ Self-healing
✅ Rolling updates
✅ Resource isolation
```

---

## Migration Timeline

| Phase | Duration | Key Tasks |
|-------|----------|-----------|
| **Week 1-2** | Infrastructure Setup | EKS cluster, operators, monitoring |
| **Week 3** | Stateless Services | Trino, Superset, Schema Registry |
| **Week 4** | Stateful Services | PostgreSQL, Hive, MinIO/S3 |
| **Week 5** | Kafka Migration | Deploy Kafka, replicate topics |
| **Week 6** | Spark Migration | Deploy Spark, streaming jobs |
| **Week 7** | Validation & Cutover | Testing, cutover, decommission |
| **Week 8+** | Optimization | Cost optimization, auto-scaling |

**Total:** 7-8 weeks for complete migration

---

## Migration Strategy

### **Phase 1: Pre-Migration**
1. Provision EKS cluster with 5-6 t3.xlarge nodes
2. Install operators: Strimzi (Kafka), Spark Operator, Prometheus
3. Configure EBS CSI driver for persistent volumes
4. Deploy monitoring stack first (Prometheus, Grafana)

### **Phase 2: Data Layer Migration**
1. Deploy PostgreSQL StatefulSet with 20GB EBS volume
2. Migrate Hive Metastore database: `pg_dump` → `pg_restore`
3. Deploy Hive Metastore, connect to PostgreSQL
4. Deploy MinIO (distributed mode) OR migrate to S3
5. Sync data: `mc mirror` (MinIO) or `aws s3 sync` (S3)

### **Phase 3: Kafka Migration**
1. Deploy Kafka cluster with Strimzi (3 brokers, KRaft mode)
2. Create topics with same configuration
3. Use MirrorMaker 2 to replicate data from old to new cluster
4. Switch producers (data-generator) to new cluster
5. Switch consumers (Spark) to new cluster
6. Validate data consistency, decommission old Kafka

### **Phase 4: Spark Migration**
1. Deploy Spark Operator
2. Upload Spark jobs and dependencies to S3/MinIO
3. Configure Spark jobs with checkpointing
4. Deploy streaming job (will resume from last checkpoint)
5. Monitor for 24-48 hours
6. Decommission old Spark cluster

### **Phase 5: Analytics Migration**
1. Deploy Trino with Iceberg catalog
2. Deploy Superset with persistent storage
3. Verify queries return correct results
4. Update dashboards and connections

### **Phase 6: Cutover**
1. Stop data generator on old system
2. Wait for Spark to process all messages (check Kafka lag)
3. Verify row counts and data consistency
4. Switch DNS/ingress to new cluster
5. Start data generator on new system
6. Monitor for 48 hours, then decommission old system

---

## Key Configuration Changes

### **Kafka Bootstrap Servers**
```
Old: kafka-1:29092,kafka-2:29092,kafka-3:29092
New: lakehouse-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
```

### **Hive Metastore URI**
```
Old: thrift://hive-metastore:9083
New: thrift://hive-metastore.storage.svc.cluster.local:9083
```

### **MinIO Endpoint (if not using S3)**
```
Old: http://minio:9000
New: http://minio.storage.svc.cluster.local:9000
```

### **S3 Endpoint (if migrating to S3)**
```
Old: s3a://lakehouse/
New: s3a://your-bucket-name/
```

### **PostgreSQL Connection**
```
Old: jdbc:postgresql://postgres:5432/metastore
New: jdbc:postgresql://postgres.storage.svc.cluster.local:5432/metastore
```

---

## Resource Allocation per Node

### **5-Node Cluster Distribution**

**Node 1: Control Plane**
- Kubernetes control plane only
- Resources: 2 cores, 8GB RAM (t3.large)

**Node 2: Kafka Primary**
- Kafka-1: 1-2 cores, 2GB RAM
- Kafka-2: 1-2 cores, 2GB RAM
- Schema Registry: 0.5 cores, 512MB RAM
- Total: ~4 cores, 5GB RAM

**Node 3: Kafka Secondary**
- Kafka-3: 1-2 cores, 2GB RAM
- Kafka UI: 0.25 cores, 256MB RAM
- JMX Exporters: 0.3 cores, 384MB RAM
- Total: ~2 cores, 3GB RAM

**Node 4: Storage**
- PostgreSQL: 0.5-1 cores, 1-2GB RAM
- Hive Metastore: 1 core, 1-2GB RAM
- MinIO: 1-2 cores, 2-4GB RAM (or use S3)
- Total: ~3-4 cores, 5-8GB RAM

**Node 5: Processing**
- Spark Master: 1 core, 1GB RAM
- Spark Worker-1: 2 cores, 4GB RAM
- Spark Worker-2: 2 cores, 4GB RAM
- Data Generator: 0.5 cores, 512MB RAM
- Total: ~5 cores, 9GB RAM

**Node 6: Analytics (Optional but Recommended)**
- Trino: 2 cores, 3-4GB RAM
- Superset: 1 core, 1-2GB RAM
- Prometheus: 0.5 cores, 2GB RAM
- Grafana: 0.25 cores, 256MB RAM
- Total: ~4 cores, 7GB RAM

---

## Cost Comparison

### **Monthly Costs**

| Configuration | On-Demand | 1yr RI | 3yr RI |
|---------------|-----------|--------|--------|
| **4 nodes (t3.xlarge)** | $480 | $288 | $192 |
| **5 nodes (t3.xlarge)** | $600 | $360 | $240 |
| **6 nodes (t3.xlarge)** | $720 | $432 | $288 |

**Add:**
- EBS volumes (300GB): ~$30/month
- Network transfer (1TB): ~$50/month
- **Total (5 nodes):** $680/month (on-demand), $440/month (1yr RI)

**Compare to:**
- Current on-prem: $200-400/month (hardware depreciation, power, maintenance)
- Managed services (MSK + RDS): $960-1085/month

**Recommendation:** Use 1-year Reserved Instances for 40% savings

---

## Disaster Recovery & High Availability

### **Current Setup**
- ❌ No redundancy
- ❌ Single point of failure
- ❌ Manual recovery
- ❌ No automated backups
- RTO: 4-8 hours, RPO: 24 hours

### **Kubernetes Setup**
- ✅ Multi-node redundancy
- ✅ Self-healing pods
- ✅ Automated failover
- ✅ Automated backups (Velero)
- ✅ Multi-AZ deployment
- RTO: <1 hour, RPO: <1 hour

### **Backup Strategy**
1. **PostgreSQL:** Daily backups to S3 (automated CronJob)
2. **Kafka:** MirrorMaker 2 to backup cluster or topic snapshots
3. **MinIO/S3:** Cross-region replication
4. **Entire cluster:** Velero backups to S3 (weekly)
5. **Hive Metastore:** Included in PostgreSQL backup

---

## Rollback Plan

If migration fails or issues arise:

1. **Keep old system running for 1-2 weeks** after cutover
2. **MirrorMaker can reverse-replicate** Kafka topics back to old cluster
3. **Database backups available** (pg_dump from before migration)
4. **DNS switch back** to old system (5 minutes)
5. **S3/MinIO data preserved** (use versioning)
6. **Total rollback time:** <1 hour

---

## Performance Comparison

| Metric | Current (Single Node) | Kubernetes (5 nodes) |
|--------|----------------------|---------------------|
| **Kafka throughput** | 333 records/sec | 333-1000 records/sec (scalable) |
| **Spark latency** | 30-second batches | 30-second batches (same) |
| **Trino query time** | <1 second (22K rows) | <1 second (same or better) |
| **Network latency** | ~0.01ms (localhost) | ~0.5-2ms (pod-to-pod) |
| **Failover time** | Manual (hours) | Automatic (seconds) |
| **Scaling time** | Manual (hours/days) | Automatic (minutes) |

**Expected impact:** Minimal performance difference, significantly improved availability

---

## Common Challenges & Solutions

### **Challenge 1: Stateful Services**
- **Issue:** Kafka, PostgreSQL, MinIO need persistent storage
- **Solution:** Use StatefulSets with EBS volumes, configure pod anti-affinity

### **Challenge 2: Inter-Service Discovery**
- **Issue:** Services need to find each other across pods
- **Solution:** Use Kubernetes Services with DNS (e.g., `kafka.namespace.svc.cluster.local`)

### **Challenge 3: Spark Checkpoint Recovery**
- **Issue:** Spark needs to resume from last checkpoint
- **Solution:** Store checkpoints in S3, configure checkpoint path in SparkApplication

### **Challenge 4: Kafka Offset Management**
- **Issue:** Consumers need to resume from correct offset
- **Solution:** Spark Structured Streaming handles this automatically with checkpoints

### **Challenge 5: Resource Constraints**
- **Issue:** Limited resources per node (4 cores, 16GB)
- **Solution:** Distribute services across nodes, use pod anti-affinity, set resource limits

---

## Success Criteria

### **Technical Validation**
- [ ] All 25+ services running and healthy
- [ ] Kafka lag < 1000 messages
- [ ] Spark streaming latency < 60 seconds
- [ ] Trino queries complete within 10% of baseline
- [ ] Zero data loss during migration
- [ ] System uptime > 99.5%

### **Operational Validation**
- [ ] Migration completed within 8 weeks
- [ ] Downtime during cutover < 4 hours
- [ ] Team trained on Kubernetes operations
- [ ] Documentation complete
- [ ] Monitoring dashboards operational
- [ ] Backup/restore tested

### **Business Validation**
- [ ] Total cost within budget ($600-800/month)
- [ ] Ability to scale to 2x current load
- [ ] Improved disaster recovery (RTO < 1 hour)
- [ ] Reduced operational burden

---

## Next Steps

1. **Get Approval**
   - Review this document with stakeholders
   - Approve budget ($600-720/month for 5-6 nodes)
   - Set migration timeline (7-8 weeks)

2. **Provision Infrastructure**
   - Create EKS cluster in AWS
   - Provision 5-6 t3.xlarge EC2 instances
   - Configure VPC, subnets, security groups
   - Install EBS CSI driver

3. **Install Operators**
   - Strimzi Kafka Operator
   - Spark Operator
   - Prometheus Operator
   - Cert-manager (for TLS)

4. **Deploy Monitoring First**
   - Prometheus
   - Grafana
   - Set up initial dashboards

5. **Follow Migration Plan**
   - See `docs/kubernetes-migration-feasibility.md` for detailed phases
   - Use manifests in `k8s/` directory
   - Test each phase before proceeding

6. **Validate & Cutover**
   - Run parallel for 1 week
   - Validate data consistency
   - Execute cutover plan
   - Monitor for 48 hours

7. **Optimize**
   - Right-size resource requests
   - Enable auto-scaling
   - Purchase Reserved Instances
   - Implement cost monitoring

---

## Key Takeaways

1. ✅ **Feasible:** Migration to Kubernetes with limited EC2 resources is viable
2. 🔢 **5-6 nodes recommended:** t3.xlarge (4 cores, 16GB RAM each)
3. 💰 **Cost:** $440-720/month depending on RI commitment
4. ⏱️ **Timeline:** 7-8 weeks for full migration
5. 📈 **Benefits:** High availability, auto-scaling, self-healing, better DR
6. 🎯 **Risk:** Medium (mitigated with proper planning and phased approach)
7. 🔄 **Rollback:** Possible within 1 hour if needed

---

## Additional Documentation

- **Detailed Feasibility Analysis:** `docs/kubernetes-migration-feasibility.md`
- **Kubernetes Manifests:** `k8s/` directory
- **Deployment Guide:** `k8s/README.md`
- **Current Architecture:** Explore with `make status` and `docker-compose ps`

---

**Questions?** Review the detailed feasibility analysis or consult the Kubernetes manifests in the `k8s/` directory.
