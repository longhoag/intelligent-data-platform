#!/bin/bash

# Start Airflow Webserver
# Run this in Terminal 1

export AIRFLOW_HOME="/Volumes/deuxSSD/Developer/intelligent-data-platform"

echo "🌐 Starting Airflow Webserver..."
echo "Access at: http://localhost:8080"
echo "Login: admin / admin"
echo ""
echo "Press Ctrl+C to stop"

airflow webserver --port 8080
