#!/bin/bash

# Docker Desktop External SSD Setup Script
# Moves Docker Desktop data to external SSD to save internal storage

set -e

EXTERNAL_SSD_PATH="/Volumes/deuxSSD"
DOCKER_DATA_PATH="/Volumes/deuxSSD/docker-desktop-data"
INTERNAL_DOCKER_PATH="$HOME/Library/Containers/com.docker.docker"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%H:%M:%S')] ERROR: $1${NC}"
}

# Check if external SSD is mounted
check_external_ssd() {
    if [ ! -d "$EXTERNAL_SSD_PATH" ]; then
        error "External SSD not found at $EXTERNAL_SSD_PATH"
        exit 1
    fi
    
    # Check available space
    available_gb=$(df -BG "$EXTERNAL_SSD_PATH" | awk 'NR==2 {print $4}' | sed 's/G//')
    if [ "$available_gb" -lt 15 ]; then
        warn "Low disk space: ${available_gb}GB available (recommended: 15GB+)"
    fi
    
    log "✅ External SSD available with ${available_gb}GB free space"
}

# Stop Docker Desktop
stop_docker() {
    log "🛑 Stopping Docker Desktop..."
    
    if pgrep -f "Docker Desktop" > /dev/null; then
        osascript -e 'quit app "Docker Desktop"'
        sleep 10
        
        # Wait for Docker to fully stop
        while pgrep -f "Docker Desktop" > /dev/null; do
            log "⏳ Waiting for Docker Desktop to stop..."
            sleep 5
        done
    fi
    
    log "✅ Docker Desktop stopped"
}

# Move Docker data to external SSD
move_docker_data() {
    log "📦 Moving Docker Desktop data to external SSD..."
    
    # Create external directory
    mkdir -p "$DOCKER_DATA_PATH"
    
    # Check if data already exists on external SSD
    if [ -d "$DOCKER_DATA_PATH/vms" ]; then
        warn "Docker data already exists on external SSD"
        read -p "Overwrite existing data? (y/N): " confirm
        if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
            log "Keeping existing external SSD data"
            return
        fi
    fi
    
    # Copy Docker data to external SSD
    if [ -d "$INTERNAL_DOCKER_PATH/Data" ]; then
        log "📋 Copying Docker data to external SSD..."
        rsync -av --progress "$INTERNAL_DOCKER_PATH/Data/" "$DOCKER_DATA_PATH/"
        
        # Backup original and create symlink
        if [ ! -L "$INTERNAL_DOCKER_PATH/Data" ]; then
            mv "$INTERNAL_DOCKER_PATH/Data" "$INTERNAL_DOCKER_PATH/Data.backup.$(date +%Y%m%d)"
            ln -s "$DOCKER_DATA_PATH" "$INTERNAL_DOCKER_PATH/Data"
        fi
        
        log "✅ Docker data moved to external SSD"
    else
        warn "No existing Docker data found"
    fi
}

# Create Docker daemon configuration for external SSD
configure_docker_daemon() {
    log "⚙️ Configuring Docker daemon for external SSD..."
    
    # Create daemon configuration
    sudo mkdir -p /etc/docker
    cat > /tmp/daemon.json << EOF
{
    "data-root": "/Volumes/deuxSSD/docker-daemon-data",
    "storage-driver": "overlay2",
    "log-driver": "json-file",
    "log-opts": {
        "max-size": "10m",
        "max-file": "3"
    },
    "experimental": false
}
EOF
    
    sudo mv /tmp/daemon.json /etc/docker/daemon.json
    mkdir -p "/Volumes/deuxSSD/docker-daemon-data"
    
    log "✅ Docker daemon configured for external SSD"
}

# Start Docker Desktop
start_docker() {
    log "🚀 Starting Docker Desktop..."
    
    open -a "Docker Desktop"
    
    # Wait for Docker to start
    log "⏳ Waiting for Docker Desktop to start..."
    while ! docker info >/dev/null 2>&1; do
        sleep 5
    done
    
    log "✅ Docker Desktop started successfully"
}

# Verify external SSD setup
verify_setup() {
    log "🔍 Verifying external SSD setup..."
    
    # Check Docker root directory
    docker_root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || echo "unknown")
    log "Docker Root Directory: $docker_root"
    
    # Check symlink
    if [ -L "$INTERNAL_DOCKER_PATH/Data" ]; then
        symlink_target=$(readlink "$INTERNAL_DOCKER_PATH/Data")
        log "Docker Data Symlink: $symlink_target"
    fi
    
    # Check disk usage
    echo
    log "📊 External SSD Usage:"
    du -sh "$DOCKER_DATA_PATH" 2>/dev/null || echo "No data yet"
    
    echo
    log "💾 Internal Storage Saved:"
    if [ -d "$INTERNAL_DOCKER_PATH/Data.backup."* ]; then
        du -sh "$INTERNAL_DOCKER_PATH/Data.backup."* 2>/dev/null || echo "No backup found"
    fi
    
    log "✅ External SSD setup verified"
}

# Restore internal storage
restore_internal() {
    log "🔄 Restoring Docker to internal storage..."
    
    stop_docker
    
    # Remove symlink and restore backup
    if [ -L "$INTERNAL_DOCKER_PATH/Data" ]; then
        rm "$INTERNAL_DOCKER_PATH/Data"
        
        # Find most recent backup
        backup_dir=$(ls -dt "$INTERNAL_DOCKER_PATH/Data.backup."* 2>/dev/null | head -1)
        if [ -n "$backup_dir" ]; then
            mv "$backup_dir" "$INTERNAL_DOCKER_PATH/Data"
            log "✅ Restored from backup: $(basename "$backup_dir")"
        fi
    fi
    
    # Remove daemon configuration
    sudo rm -f /etc/docker/daemon.json
    
    start_docker
    log "✅ Docker restored to internal storage"
}

# Show usage statistics
show_usage() {
    log "📊 Docker External SSD Usage Report"
    echo
    
    echo "🐳 Docker Info:"
    docker info --format "Root Dir: {{.DockerRootDir}}" 2>/dev/null || echo "Docker not running"
    
    echo
    echo "📁 External SSD Docker Data:"
    du -sh "$DOCKER_DATA_PATH" 2>/dev/null || echo "No data found"
    
    echo
    echo "💽 External SSD Free Space:"
    df -h "$EXTERNAL_SSD_PATH" | awk 'NR==2 {print "Available: " $4 " / " $2}'
    
    echo
    echo "🔗 Symlink Status:"
    if [ -L "$INTERNAL_DOCKER_PATH/Data" ]; then
        echo "✅ Symlinked to: $(readlink "$INTERNAL_DOCKER_PATH/Data")"
    else
        echo "❌ Not symlinked (using internal storage)"
    fi
}

# Main function
main() {
    case "${1:-help}" in
        setup)
            check_external_ssd
            stop_docker
            move_docker_data
            configure_docker_daemon
            start_docker
            verify_setup
            echo
            log "🎉 Docker Desktop is now using external SSD!"
            ;;
        restore)
            restore_internal
            ;;
        verify)
            verify_setup
            ;;
        usage)
            show_usage
            ;;
        help|*)
            echo "Docker Desktop External SSD Setup"
            echo
            echo "Usage: $0 {setup|restore|verify|usage|help}"
            echo
            echo "Commands:"
            echo "  setup    - Move Docker Desktop to external SSD"
            echo "  restore  - Restore Docker Desktop to internal storage"  
            echo "  verify   - Verify external SSD configuration"
            echo "  usage    - Show usage statistics"
            echo "  help     - Show this help message"
            echo
            echo "External SSD Path: $EXTERNAL_SSD_PATH"
            ;;
    esac
}

main "$@"
