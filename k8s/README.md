# Kubernetes Manifests for Data Lakehouse Migration

This directory contains Kubernetes manifests for migrating the on-prem data lakehouse to EKS.

## Directory Structure

```
k8s/
├── README.md                          # This file
├── namespaces/
│   └── all-namespaces.yaml           # Create all required namespaces
├── storage/
│   └── storage-classes.yaml          # EBS storage classes (fast-ssd, standard, high-throughput)
├── storage-layer/
│   ├── postgres.yaml                 # PostgreSQL StatefulSet for Hive Metastore
│   ├── hive-metastore.yaml          # Hive Metastore Deployment (TODO)
│   └── minio.yaml                    # MinIO StatefulSet (TODO) or use S3
├── kafka/
│   ├── strimzi-kafka-cluster.yaml   # Kafka cluster with KRaft mode (3 brokers)
│   └── schema-registry.yaml         # Confluent Schema Registry
├── spark/
│   ├── spark-on-k8s.yaml            # Spark Operator and streaming job
│   └── batch-jobs.yaml              # Batch aggregation jobs (TODO)
├── analytics/
│   ├── trino.yaml                   # Trino Deployment
│   └── superset.yaml                # Apache Superset (TODO)
├── monitoring/
│   ├── prometheus-stack.yaml        # Prometheus Operator stack (TODO)
│   └── servicemonitors/             # ServiceMonitors for each service (TODO)
├── ingress/
│   └── ingress.yaml                 # Ingress rules for web UIs (TODO)
└── data-generator/
    └── data-generator.yaml          # Python data generator (TODO)
```

## Prerequisites

1. **EKS Cluster** with 5-6 worker nodes (t3.xlarge)
2. **EBS CSI Driver** installed
3. **Strimzi Kafka Operator** installed
4. **Spark Operator** installed (for Spark jobs)
5. **kubectl** configured to access the cluster
6. **Helm 3** installed

## Installation Order

### Step 1: Install Operators

```bash
# Install EBS CSI Driver
kubectl apply -k "github.com/kubernetes-sigs/aws-ebs-csi-driver/deploy/kubernetes/overlays/stable/?ref=release-1.25"

# Install Strimzi Kafka Operator
kubectl create namespace kafka
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Install Spark Operator
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  --set webhook.enable=true

# Install Prometheus Operator (kube-prometheus-stack)
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install kube-prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace
```

### Step 2: Create Namespaces

```bash
kubectl apply -f namespaces/all-namespaces.yaml
```

### Step 3: Create Storage Classes

```bash
kubectl apply -f storage/storage-classes.yaml
```

### Step 4: Deploy Storage Layer

```bash
# Deploy PostgreSQL
kubectl apply -f storage-layer/postgres.yaml

# Wait for PostgreSQL to be ready
kubectl wait --for=condition=ready pod -l app=postgres -n storage --timeout=300s

# Deploy Hive Metastore
kubectl apply -f storage-layer/hive-metastore.yaml

# Deploy MinIO (or configure S3)
kubectl apply -f storage-layer/minio.yaml
```

### Step 5: Deploy Kafka Cluster

```bash
# Deploy Kafka cluster with KRaft mode
kubectl apply -f kafka/strimzi-kafka-cluster.yaml

# Wait for Kafka cluster to be ready (this may take 5-10 minutes)
kubectl wait kafka/lakehouse-cluster --for=condition=Ready --timeout=600s -n kafka

# Deploy Schema Registry
kubectl apply -f kafka/schema-registry.yaml

# Verify Kafka is running
kubectl get kafka -n kafka
kubectl get pods -n kafka
```

### Step 6: Deploy Spark

```bash
# Apply Spark RBAC and config
kubectl apply -f spark/spark-on-k8s.yaml

# The SparkApplication CRD will automatically create driver and executor pods
# Monitor the streaming job
kubectl get sparkapplications -n spark
kubectl logs -f <driver-pod-name> -n spark
```

### Step 7: Deploy Analytics Layer

```bash
# Deploy Trino
kubectl apply -f analytics/trino.yaml

# Deploy Superset
kubectl apply -f analytics/superset.yaml

# Verify services
kubectl get svc -n analytics
```

### Step 8: Deploy Monitoring

```bash
# Prometheus and Grafana are already installed via kube-prometheus-stack
# Access Grafana
kubectl port-forward svc/kube-prometheus-grafana 3000:80 -n monitoring

# Default credentials: admin/prom-operator
```

### Step 9: Deploy Ingress (Optional)

```bash
# Install NGINX Ingress Controller
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx --create-namespace

# Apply ingress rules
kubectl apply -f ingress/ingress.yaml
```

### Step 10: Deploy Data Generator

```bash
kubectl apply -f data-generator/data-generator.yaml
```

## Verification

### Check all pods are running

```bash
kubectl get pods -A | grep -E 'kafka|spark|storage|analytics|monitoring'
```

### Test Kafka

```bash
# Get Kafka bootstrap service
kubectl get svc lakehouse-cluster-kafka-bootstrap -n kafka

# Test from within cluster
kubectl run kafka-test -it --rm --image=confluentinc/cp-kafka:7.5.0 -n kafka -- /bin/bash
kafka-topics.sh --bootstrap-server lakehouse-cluster-kafka-bootstrap:9092 --list
```

### Test Trino

```bash
# Port-forward Trino
kubectl port-forward svc/trino 8080:8080 -n analytics

# Connect via CLI
docker run -it --rm --network host trinodb/trino:latest trino --server http://localhost:8080

# Run a query
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SELECT COUNT(*) FROM iceberg.bronze.transactions;
```

### Test Spark Streaming

```bash
# Check Spark application status
kubectl get sparkapplications -n spark

# View driver logs
kubectl logs -f $(kubectl get pods -n spark -l spark-role=driver --no-headers | awk '{print $1}') -n spark

# Check Spark UI (port-forward)
kubectl port-forward svc/kafka-to-iceberg-streaming-ui-svc 4040:4040 -n spark
```

## Monitoring

### Access Grafana

```bash
kubectl port-forward svc/kube-prometheus-grafana 3000:80 -n monitoring
# Open http://localhost:3000
# Username: admin, Password: prom-operator
```

### Access Prometheus

```bash
kubectl port-forward svc/kube-prometheus-prometheus 9090:9090 -n monitoring
# Open http://localhost:9090
```

### Access Kafka UI (if deployed)

```bash
kubectl port-forward svc/kafka-ui 8080:8080 -n kafka
# Open http://localhost:8080
```

## Troubleshooting

### Pods stuck in Pending

```bash
# Check events
kubectl get events -n <namespace> --sort-by='.lastTimestamp'

# Check pod description
kubectl describe pod <pod-name> -n <namespace>

# Common issues:
# - Insufficient resources: scale up cluster or adjust resource requests
# - PVC not bound: check storage class and PVC status
# - Node selector not matching: remove or adjust nodeSelector
```

### Kafka not starting

```bash
# Check Kafka cluster status
kubectl get kafka lakehouse-cluster -n kafka -o yaml

# Check Kafka operator logs
kubectl logs deployment/strimzi-cluster-operator -n kafka

# Check individual broker logs
kubectl logs lakehouse-cluster-kafka-0 -n kafka
```

### Spark job failing

```bash
# Check SparkApplication status
kubectl describe sparkapplication kafka-to-iceberg-streaming -n spark

# Check driver logs
kubectl logs -f $(kubectl get pods -n spark -l spark-role=driver --no-headers | awk '{print $1}') -n spark

# Common issues:
# - JAR dependencies missing: ensure all JARs are in S3/MinIO
# - Kafka connection issues: verify Kafka bootstrap servers
# - S3 access denied: check S3 credentials or IRSA configuration
```

### Trino query failures

```bash
# Check Trino logs
kubectl logs deployment/trino -n analytics

# Common issues:
# - Hive Metastore connection: verify thrift://hive-metastore.storage.svc.cluster.local:9083
# - S3/MinIO credentials: check catalog configuration
# - Out of memory: increase memory limits
```

## Migration Checklist

- [ ] EKS cluster provisioned with 5-6 t3.xlarge nodes
- [ ] All operators installed (EBS CSI, Strimzi, Spark Operator, Prometheus)
- [ ] Namespaces created
- [ ] Storage classes configured
- [ ] PostgreSQL deployed and healthy
- [ ] Hive Metastore deployed and connected to PostgreSQL
- [ ] MinIO deployed (or S3 configured)
- [ ] Kafka cluster deployed and healthy (3 brokers)
- [ ] Kafka topics created
- [ ] Schema Registry deployed
- [ ] Spark Operator configured
- [ ] Spark streaming job deployed
- [ ] Trino deployed and connected to Hive Metastore
- [ ] Monitoring stack deployed (Prometheus, Grafana)
- [ ] Data generator deployed
- [ ] All services verified and healthy
- [ ] Data migration completed (Kafka topics, S3 data, PostgreSQL data)
- [ ] Cutover plan executed
- [ ] Old system decommissioned

## Cost Optimization

### Use Spot Instances for Worker Nodes

```bash
# Update EKS node group to use Spot Instances
# 50-70% cost savings for non-critical workloads
```

### Use Reserved Instances

```bash
# Purchase 1-year or 3-year Reserved Instances for predictable workloads
# 40-60% cost savings
```

### Enable Cluster Autoscaler

```bash
# Install cluster autoscaler
kubectl apply -f https://raw.githubusercontent.com/kubernetes/autoscaler/master/cluster-autoscaler/cloudprovider/aws/examples/cluster-autoscaler-autodiscover.yaml

# Automatically scale nodes based on workload
```

## Backup & Disaster Recovery

### Backup PostgreSQL

```bash
# Automated backups using CronJob
kubectl create -f backup/postgres-backup-cronjob.yaml
```

### Backup Kafka Topics

```bash
# Use MirrorMaker 2 to replicate to backup cluster
# Or use Kafka topic snapshots to S3
```

### Backup Entire Cluster with Velero

```bash
# Install Velero
velero install --provider aws --bucket lakehouse-backups --secret-file ./credentials-velero

# Create backup
velero backup create lakehouse-backup --include-namespaces kafka,spark,storage,analytics

# Restore from backup
velero restore create --from-backup lakehouse-backup
```

## Next Steps

1. Complete remaining manifests (marked as TODO)
2. Set up CI/CD pipeline for automated deployments
3. Implement GitOps with ArgoCD or FluxCD
4. Configure auto-scaling for Spark and Trino
5. Set up alerting rules in Prometheus
6. Implement disaster recovery procedures
7. Conduct load testing
8. Optimize resource requests/limits based on actual usage

## Additional Resources

- [Strimzi Kafka Documentation](https://strimzi.io/docs/operators/latest/overview.html)
- [Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Trino on Kubernetes](https://trino.io/docs/current/installation/kubernetes.html)
- [EKS Best Practices](https://aws.github.io/aws-eks-best-practices/)
- [Apache Iceberg](https://iceberg.apache.org/)
