#!/bin/bash

# ============================================================================
# Flink Job Build and Submission Script
# ============================================================================
# This script builds the Flink job JAR and submits jobs to the Flink cluster
#
# Usage:
#   ./build-and-submit.sh [job_name]
#
# Available jobs:
#   - fraud-detection
#   - transaction-monitoring
#   - balance-aggregation
#   - all (submits all jobs)
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
FLINK_JOBMANAGER="flink-jobmanager"
JOBS_DIR="/opt/flink-jobs"
TARGET_JAR="flink-banking-pipelines-1.0.0.jar"

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to build the Flink jobs
build_jobs() {
    print_info "Building Flink jobs..."

    docker exec -it ${FLINK_JOBMANAGER} bash -c "
        cd ${JOBS_DIR} && \
        mvn clean package -DskipTests
    "

    if [ $? -eq 0 ]; then
        print_info "✓ Build completed successfully"
    else
        print_error "✗ Build failed"
        exit 1
    fi
}

# Function to submit a Flink job
submit_job() {
    local job_name=$1
    local main_class=$2

    print_info "Submitting ${job_name}..."

    docker exec -it ${FLINK_JOBMANAGER} bash -c "
        /opt/flink/bin/flink run \
            -d \
            -c ${main_class} \
            ${JOBS_DIR}/target/${TARGET_JAR}
    "

    if [ $? -eq 0 ]; then
        print_info "✓ ${job_name} submitted successfully"
    else
        print_error "✗ Failed to submit ${job_name}"
        exit 1
    fi
}

# Function to list running jobs
list_jobs() {
    print_info "Currently running Flink jobs:"
    docker exec -it ${FLINK_JOBMANAGER} /opt/flink/bin/flink list -r
}

# Main script logic
main() {
    local job=${1:-all}

    print_info "==================================="
    print_info "Flink Job Build & Submission Tool"
    print_info "==================================="

    # Build jobs
    build_jobs

    # Submit based on argument
    case $job in
        fraud-detection)
            submit_job "Fraud Detection Job" "com.banking.flink.jobs.FraudDetectionJob"
            ;;
        transaction-monitoring)
            submit_job "Transaction Monitoring Job" "com.banking.flink.jobs.TransactionMonitoringJob"
            ;;
        balance-aggregation)
            submit_job "Balance Aggregation Job" "com.banking.flink.jobs.BalanceAggregationJob"
            ;;
        all)
            print_info "Submitting all jobs..."
            submit_job "Fraud Detection Job" "com.banking.flink.jobs.FraudDetectionJob"
            submit_job "Transaction Monitoring Job" "com.banking.flink.jobs.TransactionMonitoringJob"
            submit_job "Balance Aggregation Job" "com.banking.flink.jobs.BalanceAggregationJob"
            ;;
        *)
            print_error "Unknown job: $job"
            print_info "Available jobs: fraud-detection, transaction-monitoring, balance-aggregation, all"
            exit 1
            ;;
    esac

    # List running jobs
    echo ""
    list_jobs

    echo ""
    print_info "==================================="
    print_info "✓ All tasks completed successfully"
    print_info "==================================="
    print_info "Flink Web UI: http://localhost:8081"
}

# Run main function
main "$@"
