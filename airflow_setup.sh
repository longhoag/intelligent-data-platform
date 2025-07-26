#!/bin/bash

# Airflow Local Setup Script for Intelligent Data Platform
# This script sets up Apache Airflow locally to run the financial data pipeline DAG

set -e

echo "🚀 Setting up Apache Airflow for Intelligent Data Platform"
echo "=========================================================="

# Set environment variables
export AIRFLOW_HOME="/Volumes/deuxSSD/Developer/intelligent-data-platform"
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_HOME/dags"
export AIRFLOW__CORE__BASE_LOG_FOLDER="$AIRFLOW_HOME/logs"
export AIRFLOW__CORE__EXECUTOR="LocalExecutor"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG="True"

echo "✅ Environment variables set"
echo "   AIRFLOW_HOME: $AIRFLOW_HOME"
echo "   DAGs Folder: $AIRFLOW__CORE__DAGS_FOLDER"

# Create necessary directories
mkdir -p logs
mkdir -p plugins
mkdir -p config

echo "✅ Created Airflow directories"

# Check if Airflow is installed
if ! command -v airflow &> /dev/null; then
    echo "📦 Installing Apache Airflow..."
    pip install apache-airflow[postgres,redis]==2.8.1
    echo "✅ Airflow installed successfully"
else
    echo "✅ Airflow is already installed"
fi

# Initialize Airflow database (only if not exists)
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "🗄️  Initializing Airflow database..."
    airflow db init
    echo "✅ Database initialized"
else
    echo "✅ Database already exists"
fi

# Create admin user (only if not exists)
if ! airflow users list | grep -q "admin"; then
    echo "👤 Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@intelligentdata.com \
        --password admin
    echo "✅ Admin user created (username: admin, password: admin)"
else
    echo "✅ Admin user already exists"
fi

# Copy configuration file if it exists
if [ -f "config/airflow.cfg" ]; then
    cp config/airflow.cfg $AIRFLOW_HOME/airflow.cfg
    echo "✅ Configuration file copied"
fi

echo ""
echo "🎉 Airflow setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Start the webserver: ./start_airflow_webserver.sh"
echo "2. Start the scheduler: ./start_airflow_scheduler.sh"
echo "3. Open Airflow UI: http://localhost:8080"
echo "4. Login with: admin / admin"
echo ""
echo "🔍 Available DAGs:"
echo "   • financial_data_pipeline - Complete financial analytics pipeline"
echo ""
