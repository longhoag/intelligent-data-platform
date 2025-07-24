#!/bin/bash

# Day 3 Streaming Infrastructure Deployment Script - External SSD Version
# Optimized for external SSD storage to preserve internal disk space

set -e

# Configuration
PROJECT_DIR="/Volumes/deuxSSD/Developer/intelligent-data-platform"
DOCKER_DATA_DIR="$PROJECT_DIR/docker-data"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.external.yml"
LOG_DIR="$PROJECT_DIR/logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

# Check external SSD space (macOS compatible)
check_external_ssd() {
    if [ ! -d "$EXTERNAL_SSD_PATH" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ External SSD not found at $EXTERNAL_SSD_PATH"
        exit 1
    fi
    
    # Use macOS compatible df command
    local available_space=$(df -g "$EXTERNAL_SSD_PATH" | awk 'NR==2 {print $4}')
    local required_space=10  # 10GB
    
    if [ "$available_space" -lt "$required_space" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ Insufficient space on external SSD"
        echo "Available: ${available_space} GB"
        echo "Required: ${required_space} GB"
        exit 1
    fi
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ External SSD space: ${available_space} GB available"
}

# Setup project directories
setup_directories() {
    log "📁 Setting up external SSD directories..."
    
    # Create all necessary directories
    mkdir -p "$DOCKER_DATA_DIR"/{kafka,zookeeper,postgres,redis,prometheus,grafana}
    mkdir -p "$LOG_DIR"
    mkdir -p "$PROJECT_DIR/docker/grafana/provisioning"/{dashboards,datasources}
    
    # Set proper permissions
    chmod -R 755 "$DOCKER_DATA_DIR"
    
    log "✅ Directories created on external SSD"
}

# Check Docker status
check_docker() {
    log "🐳 Checking Docker status..."
    
    if ! docker --version >/dev/null 2>&1; then
        error "Docker not installed or not in PATH"
        exit 1
    fi
    
    if ! docker info >/dev/null 2>&1; then
        error "Docker daemon not running"
        error "Please start Docker Desktop"
        exit 1
    fi
    
    # Check Docker resource allocation
    docker_info=$(docker system info 2>/dev/null || echo "")
    if echo "$docker_info" | grep -q "Total Memory"; then
        memory_gb=$(echo "$docker_info" | grep "Total Memory" | awk '{print $3}' | sed 's/GiB//')
        if [ "${memory_gb%.*}" -lt 4 ]; then
            warn "Docker has less than 4GB memory allocated"
            warn "Consider increasing Docker memory for better performance"
        fi
    fi
    
    log "✅ Docker is running"
}

# Create Prometheus configuration
create_prometheus_config() {
    log "📊 Creating Prometheus configuration..."
    
    cat > "$PROJECT_DIR/docker/prometheus.yml" << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'kafka-exporter'
    static_configs:
      - targets: ['kafka:9101']

  - job_name: 'postgres-exporter'
    static_configs:
      - targets: ['postgres:5432']

  - job_name: 'redis-exporter'
    static_configs:
      - targets: ['redis:6379']

  - job_name: 'streaming-app'
    static_configs:
      - targets: ['host.docker.internal:8000']
    metrics_path: '/metrics'
    scrape_interval: 5s
EOF

    log "✅ Prometheus configuration created"
}

# Start services
start_services() {
    log "🚀 Starting Day 3 streaming infrastructure on external SSD..."
    
    cd "$PROJECT_DIR"
    
    # Pull images first to show progress
    log "📥 Pulling Docker images..."
    docker-compose -f "$COMPOSE_FILE" pull
    
    # Start core services first
    log "🔧 Starting core services (Zookeeper, Kafka)..."
    docker-compose -f "$COMPOSE_FILE" up -d zookeeper
    
    # Wait for Zookeeper
    log "⏳ Waiting for Zookeeper to be ready..."
    sleep 10
    
    docker-compose -f "$COMPOSE_FILE" up -d kafka
    
    # Wait for Kafka
    log "⏳ Waiting for Kafka to be ready..."
    sleep 15
    
    # Start remaining services
    log "🔧 Starting remaining services..."
    docker-compose -f "$COMPOSE_FILE" up -d
    
    log "✅ All services started successfully"
}

# Stop services
stop_services() {
    log "🛑 Stopping Day 3 streaming infrastructure..."
    
    cd "$PROJECT_DIR"
    docker-compose -f "$COMPOSE_FILE" down -v
    
    log "✅ All services stopped"
}

# Check service health
check_health() {
    log "🏥 Checking service health..."
    
    cd "$PROJECT_DIR"
    
    # Check running containers
    echo
    echo "📊 Container Status:"
    docker-compose -f "$COMPOSE_FILE" ps
    
    echo
    echo "💾 External SSD Usage:"
    du -sh "$DOCKER_DATA_DIR"/* 2>/dev/null || echo "No data yet"
    
    echo
    echo "🔗 Service URLs:"
    echo "  • Kafka UI: http://localhost:8080"
    echo "  • Grafana: http://localhost:3000 (admin/admin123)"
    echo "  • Prometheus: http://localhost:9090"
    echo "  • Schema Registry: http://localhost:8081"
    echo "  • KSQL: http://localhost:8088"
}

# Create sample topics
create_topics() {
    log "📝 Creating Kafka topics..."
    
    # Wait for Kafka to be fully ready
    sleep 5
    
    # Create topics
    docker exec kafka kafka-topics --create --topic market-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || true
    docker exec kafka kafka-topics --create --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || true
    docker exec kafka kafka-topics --create --topic portfolio-updates --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || true
    docker exec kafka kafka-topics --create --topic anomaly-alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null || true
    
    log "✅ Kafka topics created"
}

# Cleanup external SSD data
cleanup() {
    log "🧹 Cleaning up external SSD data..."
    
    read -p "Are you sure you want to delete all data on external SSD? (y/N): " confirm
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        stop_services
        rm -rf "$DOCKER_DATA_DIR"/*
        log "✅ External SSD data cleaned"
    else
        log "Cleanup cancelled"
    fi
}

# Show external SSD usage
show_usage() {
    log "📊 External SSD Docker Usage Report"
    echo
    
    echo "📁 Directory Sizes:"
    du -sh "$DOCKER_DATA_DIR"/* 2>/dev/null || echo "No data directories found"
    
    echo
    echo "💽 Total Docker Data Size:"
    du -sh "$DOCKER_DATA_DIR" 2>/dev/null || echo "0B"
    
    echo
    echo "🗄️ External SSD Free Space:"
    df -h /Volumes/deuxSSD | awk 'NR==2 {print "  Available: " $4 " / " $2}'
    
    echo
    echo "🐳 Docker Images Size:"
    docker system df
}

# Main function
main() {
    case "${1:-help}" in
        start)
            check_external_ssd
            setup_directories
            check_docker
            create_prometheus_config
            start_services
            create_topics
            check_health
            echo
            log "🎉 Day 3 Streaming Infrastructure is ready on external SSD!"
            log "🌐 Access Kafka UI at: http://localhost:8080"
            ;;
        stop)
            stop_services
            ;;
        restart)
            stop_services
            sleep 5
            main start
            ;;
        status|health)
            check_health
            ;;
        topics)
            create_topics
            ;;
        cleanup)
            cleanup
            ;;
        usage)
            show_usage
            ;;
        help|*)
            echo "Day 3 Streaming Infrastructure - External SSD Deployment"
            echo
            echo "Usage: $0 {start|stop|restart|status|topics|cleanup|usage|help}"
            echo
            echo "Commands:"
            echo "  start    - Start all streaming services on external SSD"
            echo "  stop     - Stop all services"
            echo "  restart  - Restart all services"
            echo "  status   - Check service health and external SSD usage"
            echo "  topics   - Create Kafka topics"
            echo "  cleanup  - Clean up all external SSD data"
            echo "  usage    - Show external SSD usage statistics"
            echo "  help     - Show this help message"
            echo
            echo "External SSD Path: /Volumes/deuxSSD/Developer/intelligent-data-platform"
            ;;
    esac
}

# Run main function
main "$@"
