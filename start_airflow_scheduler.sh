#!/bin/bash

# Start Airflow Scheduler
# Run this in Terminal 2

export AIRFLOW_HOME="/Volumes/deuxSSD/Developer/intelligent-data-platform"

echo "⏰ Starting Airflow Scheduler..."
echo "This runs the DAGs on schedule"
echo ""
echo "Press Ctrl+C to stop"

airflow scheduler
