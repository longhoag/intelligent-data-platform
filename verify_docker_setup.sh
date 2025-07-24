#!/bin/bash

# Docker External SSD Verification Script
# Checks Docker configuration and external SSD usage

set -e

EXTERNAL_SSD_PATH="/Volumes/deuxSSD"
PROJECT_DIR="/Volumes/deuxSSD/Developer/intelligent-data-platform"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%H:%M:%S')] $1${NC}"
}

info() {
    echo -e "${BLUE}[$(date +'%H:%M:%S')] $1${NC}"
}

echo "🔍 Docker External SSD Configuration Report"
echo "=" * 50

# Check external SSD
log "📁 External SSD Status"
if [ -d "$EXTERNAL_SSD_PATH" ]; then
    echo "✅ External SSD mounted at: $EXTERNAL_SSD_PATH"
    df -h "$EXTERNAL_SSD_PATH" | awk 'NR==2 {print "   Available: " $4 " / " $2 " (" $5 " used)"}'
else
    error "❌ External SSD not found at $EXTERNAL_SSD_PATH"
fi

# Check Docker status
echo
log "🐳 Docker Status"
if command -v docker >/dev/null 2>&1; then
    echo "✅ Docker installed: $(docker --version)"
    
    if docker info >/dev/null 2>&1; then
        echo "✅ Docker daemon running"
        
        # Check Docker root directory
        docker_root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || echo "unknown")
        if [[ "$docker_root" == *"/Volumes/deuxSSD"* ]]; then
            echo "✅ Docker using external SSD: $docker_root"
        else
            warn "⚠️  Docker using internal storage: $docker_root"
        fi
        
        # Check Docker data usage
        echo "📊 Docker system usage:"
        docker system df 2>/dev/null || echo "   Unable to get Docker system usage"
    else
        warn "⚠️  Docker daemon not running"
    fi
else
    error "❌ Docker not installed"
fi

# Check project structure
echo
log "📂 Project Structure"
if [ -d "$PROJECT_DIR" ]; then
    echo "✅ Project directory: $PROJECT_DIR"
    
    # Check docker-data directory
    if [ -d "$PROJECT_DIR/docker-data" ]; then
        echo "✅ Docker data directory created"
        
        echo "📊 Docker data sizes:"
        du -sh "$PROJECT_DIR/docker-data"/* 2>/dev/null || echo "   No data yet"
        
        total_size=$(du -sh "$PROJECT_DIR/docker-data" 2>/dev/null | awk '{print $1}')
        echo "   Total: $total_size"
    else
        warn "⚠️  Docker data directory not found"
    fi
    
    # Check compose files
    if [ -f "$PROJECT_DIR/docker-compose.external.yml" ]; then
        echo "✅ External SSD compose file exists"
    else
        warn "⚠️  External SSD compose file missing"
    fi
    
    if [ -f "$PROJECT_DIR/deploy_streaming_external.sh" ]; then
        echo "✅ External SSD deployment script exists"
    else
        warn "⚠️  External SSD deployment script missing"
    fi
else
    error "❌ Project directory not found: $PROJECT_DIR"
fi

# Check Docker Desktop data location
echo
log "🖥️  Docker Desktop Data Location"
docker_desktop_data="$HOME/Library/Containers/com.docker.docker/Data"
if [ -L "$docker_desktop_data" ]; then
    target=$(readlink "$docker_desktop_data")
    if [[ "$target" == *"/Volumes/deuxSSD"* ]]; then
        echo "✅ Docker Desktop data symlinked to external SSD: $target"
    else
        warn "⚠️  Docker Desktop data symlinked but not to external SSD: $target"
    fi
elif [ -d "$docker_desktop_data" ]; then
    size=$(du -sh "$docker_desktop_data" 2>/dev/null | awk '{print $1}')
    warn "⚠️  Docker Desktop using internal storage: $size"
else
    info "ℹ️  Docker Desktop data directory not found"
fi

# Check running containers
echo
log "🏃 Running Containers"
if docker ps >/dev/null 2>&1; then
    container_count=$(docker ps -q | wc -l | tr -d ' ')
    if [ "$container_count" -gt 0 ]; then
        echo "✅ Running containers: $container_count"
        docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null
    else
        info "ℹ️  No containers currently running"
    fi
else
    info "ℹ️  Unable to check running containers"
fi

# Check volumes
echo
log "💾 Docker Volumes"
if docker volume ls >/dev/null 2>&1; then
    volume_count=$(docker volume ls -q | wc -l | tr -d ' ')
    echo "📊 Total volumes: $volume_count"
    
    # Check for external SSD volumes
    external_volumes=$(docker volume ls --format "table {{.Name}}\t{{.Driver}}\t{{.Mountpoint}}" 2>/dev/null | grep -c "/Volumes/deuxSSD" || echo "0")
    if [ "$external_volumes" -gt 0 ]; then
        echo "✅ External SSD volumes: $external_volumes"
    else
        info "ℹ️  No volumes on external SSD yet"
    fi
else
    info "ℹ️  Unable to check volumes"
fi

# Summary
echo
echo "=" * 50
log "📋 Summary"

if [ -d "$EXTERNAL_SSD_PATH" ] && [ -d "$PROJECT_DIR/docker-data" ]; then
    echo "✅ External SSD setup: Ready"
else
    echo "❌ External SSD setup: Incomplete"
fi

if docker info >/dev/null 2>&1; then
    echo "✅ Docker: Running"
else
    echo "❌ Docker: Not running"
fi

echo
info "🚀 To start the external SSD streaming infrastructure:"
echo "   cd $PROJECT_DIR"
echo "   ./deploy_streaming_external.sh start"
echo
info "📊 To monitor external SSD usage:"
echo "   ./verify_docker_setup.sh"
