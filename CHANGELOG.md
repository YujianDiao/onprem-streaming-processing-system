# Changelog - On-Prem Data Lakehouse Platform

## [Unreleased] - 2025-11-15

### Fixed

#### 🔧 Docker Compose v2 Migration
- **Changed**: Updated all `docker-compose` commands to `docker compose` (v2 syntax)
- **Removed**: Obsolete `version: '3.8'` attribute from docker-compose.yml
- **Files**: `Makefile`, `scripts/startup.sh`, `docker-compose.yml`
- **Impact**: Eliminates deprecation warnings, ensures modern Docker compatibility

#### 🔴 Hive Metastore PostgreSQL Driver (CRITICAL FIX)
- **Problem**: Service crashing with `ClassNotFoundException: org.postgresql.Driver`
- **Root Cause**: Apache Hive 4.0.0 base image missing PostgreSQL JDBC driver
- **Solution**: Created custom Docker image with driver and automated schema initialization
- **Added Files**:
  - `hive-metastore/Dockerfile` - Custom image with PostgreSQL JDBC driver
  - `hive-metastore/entrypoint.sh` - Automated schema initialization script
- **Modified Files**: `docker-compose.yml` - Updated to build custom image
- **Impact**: Hive Metastore now starts successfully, enables Iceberg catalog management

#### 🔄 Spark Image Migration (bitnami → apache)
- **Problem**: `bitnami/spark:3.5.0` image not found (discontinued by Bitnami)
- **Solution**: Migrated to official Apache Spark image `apache/spark:3.5.0`
- **Changes**:
  - Updated image from `bitnami/spark:3.5.0` to `apache/spark:3.5.0`
  - Changed volume paths from `/opt/bitnami/spark` to `/opt/spark`
  - Modified startup commands to use Spark's native sbin scripts
  - Updated worker ports from 8081/8082 to 8091/8092 (avoid schema-registry conflict)
- **Files**: `docker-compose.yml`
- **Impact**: Spark cluster starts successfully with official Apache images

#### 📦 MinIO Client Command Update
- **Problem**: Deprecated `mc config` command showing errors
- **Solution**: Updated to modern `mc alias set` syntax
- **Files**: `docker-compose.yml`
- **Impact**: Clean MinIO bucket initialization without errors

#### 📚 Documentation Updates
- **Updated**: README.md with corrected service URLs and Spark image info
- **Updated**: QUICK_START.md with Docker Compose v2 commands and new ports
- **Updated**: Makefile URLs command with correct Spark worker ports
- **Added**: FIXES_APPLIED.md - Detailed technical documentation
- **Added**: QUICK_FIX_SUMMARY.md - Quick reference guide
- **Added**: CHANGELOG.md - This file

### Changed

#### Service Port Mappings
- **Spark Worker 1**: Changed from `8081` → `8091`
- **Spark Worker 2**: Changed from `8082` → `8092`
- **Reason**: Port 8081 conflicts with Confluent Schema Registry

#### Volume Mappings (Spark)
- **Changed**: `/opt/bitnami/spark/*` → `/opt/spark/*`
- **Reason**: Apache Spark official image uses different paths than Bitnami

### Technical Details

#### Custom Hive Metastore Image
```dockerfile
FROM apache/hive:4.0.0
- Installs PostgreSQL client tools (pg_isready)
- Downloads PostgreSQL JDBC driver (v42.7.1)
- Includes custom entrypoint for automated schema initialization
- Runs as hive user for security
```

#### Spark Startup Commands
```yaml
Master: /opt/spark/sbin/start-master.sh
Worker: /opt/spark/sbin/start-worker.sh spark://spark-master:7077
```

### Deployment

#### Complete Service List (Running)
```
✅ kafka-1, kafka-2, kafka-3  (KRaft mode)
✅ schema-registry            (Confluent)
✅ kafka-ui                   (Provectus)
✅ postgres                   (PostgreSQL 15)
✅ minio                      (S3-compatible storage)
✅ hive-metastore            (Custom build with PostgreSQL driver)
✅ spark-master              (Apache Spark 3.5.0)
✅ spark-worker-1            (Apache Spark 3.5.0)
✅ spark-worker-2            (Apache Spark 3.5.0)
```

#### Service URLs (Updated)
| Service | Old Port | New Port | URL |
|---------|----------|----------|-----|
| Kafka UI | 8080 | 8080 | http://localhost:8080 |
| Schema Registry | 8081 | 8081 | http://localhost:8081 |
| Spark Master | 8888 | 8888 | http://localhost:8888 |
| Spark Worker 1 | 8081 | **8091** | http://localhost:8091 |
| Spark Worker 2 | 8082 | **8092** | http://localhost:8092 |
| Trino | 8086 | 8086 | http://localhost:8086 |
| MinIO Console | 9001 | 9001 | http://localhost:9001 |
| Grafana | 3000 | 3000 | http://localhost:3000 |
| Prometheus | 9090 | 9090 | http://localhost:9090 |

### Breaking Changes

⚠️ **Spark Worker UI Ports Changed**
- Old: Worker 1 on 8081, Worker 2 on 8082
- New: Worker 1 on 8091, Worker 2 on 8092
- **Action Required**: Update any scripts or bookmarks that reference old ports

⚠️ **Docker Compose Command**
- Old: `docker-compose` (v1)
- New: `docker compose` (v2)
- **Action Required**: Update CI/CD pipelines if using old syntax

### Migration Guide

#### For Existing Deployments

1. **Stop existing services**:
   ```bash
   docker compose down
   ```

2. **Pull latest changes**:
   ```bash
   git pull origin main
   ```

3. **Build custom Hive Metastore image**:
   ```bash
   docker compose build hive-metastore
   ```

4. **Start services**:
   ```bash
   make start
   ```

5. **Verify all services are healthy**:
   ```bash
   make status
   make test-all
   ```

### Rollback Instructions

If you need to rollback:

```bash
# Stop services
docker compose down

# Checkout previous version
git checkout <previous-commit-hash>

# Start with old configuration
docker compose up -d
```

### Known Issues

None at this time.

### Roadmap

- [ ] Add Trino startup and configuration
- [ ] Start monitoring stack (Prometheus + Grafana)
- [ ] Deploy data generator
- [ ] Complete end-to-end testing
- [ ] Add pre-built Grafana dashboards
- [ ] Document Spark job submission process

---

## Version Information

- **Docker Compose Format**: v2 (no version attribute)
- **Kafka**: 7.5.0 (Confluent Platform, KRaft mode)
- **Spark**: 3.5.0 (apache/spark official image)
- **Hive Metastore**: 4.0.0 (custom build)
- **PostgreSQL**: 15-alpine
- **MinIO**: latest
- **Trino**: latest

---

## Contributors

- System fixes and updates: 2025-11-15

---

**Status**: ✅ All core infrastructure services healthy and operational
