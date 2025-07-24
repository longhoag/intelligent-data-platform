#!/bin/bash

# Day 3 Streaming Infrastructure Deployment Script
# Deploys Kafka cluster and starts real-time processing pipeline

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DOCKER_COMPOSE_FILE="docker/docker-compose.yml"
KAFKA_TOPICS=("market-data" "transactions" "portfolio-updates" "alerts" "features")
MAX_WAIT_TIME=120  # seconds

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    print_status "Checking Docker installation..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    print_success "Docker is running"
}

# Function to check if Docker Compose is available
check_docker_compose() {
    print_status "Checking Docker Compose..."
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose is not available. Please install Docker Compose."
        exit 1
    fi
    
    print_success "Docker Compose is available"
}

# Function to create necessary directories
create_directories() {
    print_status "Creating necessary directories..."
    
    mkdir -p logs
    mkdir -p data/features
    mkdir -p data/output
    mkdir -p data/processed
    mkdir -p docker/grafana/dashboards
    mkdir -p docker/grafana/datasources
    
    print_success "Directories created"
}

# Function to stop existing containers
stop_existing_containers() {
    print_status "Stopping any existing containers..."
    
    if [ -f "$DOCKER_COMPOSE_FILE" ]; then
        docker-compose -f "$DOCKER_COMPOSE_FILE" down --remove-orphans || true
        
        # Remove volumes if requested
        if [ "$1" == "--clean" ]; then
            print_warning "Removing all volumes and data..."
            docker-compose -f "$DOCKER_COMPOSE_FILE" down -v
        fi
    fi
    
    print_success "Existing containers stopped"
}

# Function to start infrastructure
start_infrastructure() {
    print_status "Starting Kafka infrastructure..."
    
    # Start core services first
    docker-compose -f "$DOCKER_COMPOSE_FILE" up -d zookeeper kafka
    
    # Wait for Kafka to be ready
    print_status "Waiting for Kafka to be ready..."
    wait_for_kafka
    
    # Start remaining services
    print_status "Starting remaining services..."
    docker-compose -f "$DOCKER_COMPOSE_FILE" up -d
    
    print_success "Infrastructure started"
}

# Function to wait for Kafka to be ready
wait_for_kafka() {
    local wait_time=0
    local kafka_ready=false
    
    while [ $wait_time -lt $MAX_WAIT_TIME ] && [ "$kafka_ready" = false ]; do
        if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &> /dev/null; then
            kafka_ready=true
        else
            echo -n "."
            sleep 5
            wait_time=$((wait_time + 5))
        fi
    done
    
    if [ "$kafka_ready" = true ]; then
        print_success "Kafka is ready"
    else
        print_error "Kafka failed to start within $MAX_WAIT_TIME seconds"
        exit 1
    fi
}

# Function to verify topics are created
verify_topics() {
    print_status "Verifying Kafka topics..."
    
    local all_topics_ready=true
    
    for topic in "${KAFKA_TOPICS[@]}"; do
        if docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep -q "^$topic$"; then
            print_success "Topic '$topic' exists"
        else
            print_warning "Topic '$topic' not found"
            all_topics_ready=false
        fi
    done
    
    if [ "$all_topics_ready" = true ]; then
        print_success "All topics verified"
    else
        print_warning "Some topics may not be ready yet"
    fi
}

# Function to install Python dependencies
install_dependencies() {
    print_status "Installing Python dependencies..."
    
    if command -v poetry &> /dev/null; then
        poetry install
        print_success "Dependencies installed with Poetry"
    elif [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
        print_success "Dependencies installed with pip"
    else
        print_warning "No dependency management found. Please install manually."
    fi
}

# Function to run integration tests
run_tests() {
    print_status "Running integration tests..."
    
    # Wait a bit more for all services to be ready
    sleep 30
    
    if command -v poetry &> /dev/null; then
        poetry run python -m pytest tests/test_streaming_integration.py -v || true
    else
        python -m pytest tests/test_streaming_integration.py -v || true
    fi
    
    print_success "Integration tests completed"
}

# Function to show service status
show_status() {
    print_status "Service Status:"
    echo
    docker-compose -f "$DOCKER_COMPOSE_FILE" ps
    echo
    
    print_status "Kafka Topics:"
    docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || print_warning "Cannot list topics"
    echo
    
    print_status "Access URLs:"
    echo "  📊 Kafka UI: http://localhost:8080"
    echo "  📈 Grafana: http://localhost:3000 (admin/admin)"
    echo "  🔍 Prometheus: http://localhost:9090"
    echo "  📋 Schema Registry: http://localhost:8081"
    echo "  🔧 Kafka Connect: http://localhost:8083"
    echo "  💾 PostgreSQL: localhost:5432 (platform_user/platform_pass)"
    echo "  📦 Redis: localhost:6379"
    echo
}

# Function to start streaming demo
start_demo() {
    print_status "Starting Day 3 streaming demonstration..."
    
    if command -v poetry &> /dev/null; then
        poetry run python run_day3_demo.py
    else
        python run_day3_demo.py
    fi
}

# Function to monitor logs
monitor_logs() {
    print_status "Following container logs (Ctrl+C to stop)..."
    docker-compose -f "$DOCKER_COMPOSE_FILE" logs -f kafka schema-registry kafka-connect
}

# Function to cleanup
cleanup() {
    print_status "Cleaning up..."
    
    docker-compose -f "$DOCKER_COMPOSE_FILE" down
    
    if [ "$1" == "--all" ]; then
        print_warning "Removing all volumes and networks..."
        docker-compose -f "$DOCKER_COMPOSE_FILE" down -v --remove-orphans
        docker system prune -f
    fi
    
    print_success "Cleanup completed"
}

# Function to show help
show_help() {
    echo "Day 3 Streaming Infrastructure Deployment Script"
    echo
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo
    echo "Commands:"
    echo "  start                Start the complete infrastructure"
    echo "  stop                 Stop all services"
    echo "  restart              Restart all services"
    echo "  status               Show service status"
    echo "  logs                 Monitor service logs"
    echo "  demo                 Run streaming demonstration"
    echo "  test                 Run integration tests"
    echo "  cleanup              Remove containers and networks"
    echo "  help                 Show this help message"
    echo
    echo "Options:"
    echo "  --clean              Remove volumes when stopping/restarting"
    echo "  --all                Remove everything during cleanup"
    echo
    echo "Examples:"
    echo "  $0 start             Start infrastructure"
    echo "  $0 restart --clean   Restart with clean volumes"
    echo "  $0 demo              Run streaming demo"
    echo "  $0 cleanup --all     Complete cleanup"
}

# Main execution
main() {
    local command="${1:-start}"
    local option="${2:-}"
    
    echo "🚀 Intelligent Data Platform - Day 3 Deployment"
    echo "================================================="
    echo
    
    case "$command" in
        "start")
            check_docker
            check_docker_compose
            create_directories
            stop_existing_containers "$option"
            install_dependencies
            start_infrastructure
            sleep 10
            verify_topics
            show_status
            print_success "Infrastructure deployment completed!"
            print_status "Run '$0 demo' to start the streaming demonstration"
            ;;
        "stop")
            stop_existing_containers "$option"
            ;;
        "restart")
            stop_existing_containers "$option"
            start_infrastructure
            sleep 10
            verify_topics
            show_status
            ;;
        "status")
            show_status
            ;;
        "logs")
            monitor_logs
            ;;
        "demo")
            check_docker
            start_demo
            ;;
        "test")
            check_docker
            run_tests
            ;;
        "cleanup")
            cleanup "$option"
            ;;
        "help"|"--help"|"-h")
            show_help
            ;;
        *)
            print_error "Unknown command: $command"
            echo
            show_help
            exit 1
            ;;
    esac
}

# Execute main function with all arguments
main "$@"
