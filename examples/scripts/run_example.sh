#!/bin/bash
# ==============================================================================
# Run Example Script - Easy execution of platform examples
# ==============================================================================
# Usage: ./examples/scripts/run_example.sh <example_name>
# Examples:
#   ./examples/scripts/run_example.sh simple_etl
#   ./examples/scripts/run_example.sh create_gold
# ==============================================================================

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
EXAMPLES_DIR="$(dirname "$SCRIPT_DIR")"
SPARK_DIR="$EXAMPLES_DIR/spark"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ ${NC}$1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Function to check if Docker containers are running
check_prerequisites() {
    print_info "Checking prerequisites..."

    # Check if Docker is running
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi

    # Check if Spark master is running
    if ! docker ps | grep -q spark-master; then
        print_error "Spark master container is not running."
        print_info "Start the platform with: make start"
        exit 1
    fi

    # Check if Hive Metastore is running
    if ! docker ps | grep -q hive-metastore; then
        print_error "Hive Metastore container is not running."
        print_info "Start the platform with: make start"
        exit 1
    fi

    print_success "All prerequisites met"
}

# Function to run Spark job
run_spark_job() {
    local job_file=$1
    local job_name=$2

    print_info "Copying $job_name to Spark master..."
    docker cp "$job_file" spark-master:/tmp/

    local filename=$(basename "$job_file")

    echo ""
    print_info "Submitting Spark job: $job_name"
    print_info "This may take a few minutes..."
    echo ""

    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 2g \
        --executor-memory 3g \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
        /tmp/$filename

    if [ $? -eq 0 ]; then
        echo ""
        print_success "Job completed successfully!"
    else
        echo ""
        print_error "Job failed. Check logs above for details."
        exit 1
    fi
}

# Main execution
main() {
    echo ""
    echo "=============================================================================="
    echo "  Data Lakehouse Platform - Example Runner"
    echo "=============================================================================="
    echo ""

    if [ $# -eq 0 ]; then
        print_error "No example specified."
        echo ""
        echo "Usage: $0 <example_name>"
        echo ""
        echo "Available examples:"
        echo "  • simple_etl       - Transform Bronze data to Silver"
        echo "  • create_gold      - Create Gold layer aggregations"
        echo "  • data_quality     - Run data quality checks"
        echo "  • time_travel      - Iceberg time travel example"
        echo ""
        exit 1
    fi

    EXAMPLE_NAME=$1

    check_prerequisites

    case $EXAMPLE_NAME in
        simple_etl)
            run_spark_job "$SPARK_DIR/simple_etl.py" "Simple ETL (Bronze → Silver)"
            ;;
        create_gold)
            run_spark_job "$SPARK_DIR/create_gold_tables.py" "Create Gold Tables"
            ;;
        *)
            print_error "Unknown example: $EXAMPLE_NAME"
            echo ""
            echo "Available examples:"
            echo "  • simple_etl"
            echo "  • create_gold"
            echo ""
            exit 1
            ;;
    esac

    echo ""
    echo "=============================================================================="
    print_success "Example completed!"
    echo "=============================================================================="
    echo ""
}

main "$@"
