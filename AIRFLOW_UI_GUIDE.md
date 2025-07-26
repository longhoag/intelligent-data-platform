# 🌐 Viewing Airflow DAG UI on Localhost - Complete Guide

## Overview
This guide shows you how to set up and access the Apache Airflow web interface to view and manage the `financial_data_pipeline` DAG.

## Prerequisites
- Python 3.11+ environment activated
- All project dependencies installed

## Step-by-Step Setup

### 1. Initial Setup (One-time)
```bash
# Navigate to project directory
cd /Volumes/deuxSSD/Developer/intelligent-data-platform

# Run the automated setup script
./airflow_setup.sh
```

**What this does:**
- Installs Apache Airflow with PostgreSQL and Redis support
- Sets up environment variables
- Initializes SQLite database for Airflow metadata
- Creates admin user (username: `admin`, password: `admin`)
- Configures the DAGs folder to point to our `dags/` directory

### 2. Start Airflow Services

**Terminal 1 - Start Webserver:**
```bash
./start_airflow_webserver.sh
```

**Terminal 2 - Start Scheduler:**
```bash
./start_airflow_scheduler.sh
```

**Expected Output:**
```
🌐 Starting Airflow Webserver...
Access at: http://localhost:8080
Login: admin / admin

[2025-07-25 16:45:00,000] {webserver.py:123} INFO - Starting the web server on port 8080
```

### 3. Access the Airflow UI

1. **Open Browser**: Navigate to `http://localhost:8080`
2. **Login**: Use credentials `admin` / `admin`
3. **Dashboard**: You'll see the Airflow dashboard with available DAGs

### 4. Navigate to Financial Pipeline DAG

#### DAGs List View
- On the main dashboard, look for `financial_data_pipeline`
- Status indicators:
  - 🟢 Green: DAG is active and healthy
  - 🔴 Red: DAG has errors or failed runs
  - ⚪ Gray: DAG is paused

#### DAG Details Page
Click on `financial_data_pipeline` to access:

**Graph View** (Recommended):
```
┌─────────────────────────┐
│ setup_financial_env     │
└──────────┬──────────────┘
           │
    ┌──────┼──────┐
    │      │      │
    ▼      ▼      ▼
┌─────┐ ┌─────┐ ┌─────┐
│ API │ │FILE │ │ DB  │
│ Ext │ │ Ext │ │ Ext │
└──┬──┘ └──┬──┘ └──┬──┘
   └───────┼───────┘
           ▼
    ┌─────────────┐
    │ Transform   │
    └──────┬──────┘
           ▼
    ┌─────────────┐
    │ Validate    │
    └──────┬──────┘
           ▼
    ┌─────────────┐
    │ Load        │
    └──────┬──────┘
           ▼
    ┌─────────────┐
    │ Report      │
    └─────────────┘
```

### 5. Running the Pipeline

#### Manual Trigger
1. **Play Button**: Click the ▶️ button next to the DAG name
2. **Trigger DAG**: Confirm in the popup dialog
3. **Monitor**: Watch tasks turn from gray → yellow (running) → green (success)

#### From Command Line
```bash
# Trigger the pipeline
airflow dags trigger financial_data_pipeline

# Check status
airflow dags state financial_data_pipeline 2025-07-25
```

### 6. Monitoring Execution

#### Real-time Monitoring
- **Graph View**: Shows current task status with color coding
- **Tree View**: Historical timeline of DAG runs
- **Gantt View**: Task duration and overlap visualization

#### Task Details
Click on any task box to see:
- **Task Instance Details**: Duration, start/end times
- **Logs**: Real-time execution logs
- **Code**: The actual Python function being executed
- **XCom**: Inter-task communication data

#### Key Metrics to Watch
- **Duration**: Normal run takes 2-5 minutes
- **Records Processed**: Should show 40,000+ financial records
- **Success Rate**: All tasks should complete successfully

### 7. Viewing Results

#### Generated Files
After successful execution, check:
```bash
# Processed data
ls -la data/processed/financial_pipeline_output_*.csv

# Execution reports
ls -la data/output/financial_pipeline_report.json

# Validation reports
ls -la /tmp/financial_validation_report.json
```

#### Performance Metrics
In the final report task logs, look for:
- Total records extracted and processed
- Data quality score (should be 85%+)
- Technical indicators calculated
- Pipeline execution time

### 8. Troubleshooting

#### DAG Not Appearing
```bash
# Check for syntax errors
python -m py_compile dags/financial_data_pipeline_dag.py

# Refresh DAGs (wait 30-60 seconds)
# Or restart scheduler: Ctrl+C then ./start_airflow_scheduler.sh
```

#### Task Failures
- **Red Task**: Click to view error logs
- **Common Issues**: Missing dependencies, path errors, data source unavailable
- **Retry**: Most tasks have 3 automatic retries configured

#### Import Errors
Ensure all dependencies are installed:
```bash
pip install pandas numpy pyyaml sqlalchemy loguru alpha-vantage
```

### 9. Advanced Features

#### Scheduling
- Current schedule: `@hourly` (runs every hour)
- To change: Edit `schedule_interval` in the DAG file
- Options: `@daily`, `@weekly`, `0 9 * * *` (9 AM daily), etc.

#### Configuration
- **DAG Configuration**: Edit `dags/financial_data_pipeline_dag.py`
- **Airflow Settings**: Edit `airflow.cfg` in the project root
- **Connection Management**: Admin → Connections in UI

#### Data Lineage
- **XCom**: View data passed between tasks
- **Task Dependencies**: Visualize data flow
- **Audit Trail**: Complete execution history

## UI Navigation Quick Reference

| Feature | Location | Purpose |
|---------|----------|---------|
| DAGs List | Main dashboard | View all available pipelines |
| Graph View | DAG → Graph | Visual task dependencies |
| Tree View | DAG → Tree | Historical run timeline |
| Task Logs | Click task box → Logs | Execution details |
| Admin Panel | Admin menu | User/connection management |
| Data Profiling | Browse → Task Instance Logs | Data quality metrics |

## Success Indicators

✅ **Healthy DAG:**
- All tasks green in Graph view
- Recent successful runs in Tree view
- No error messages in logs
- Financial data files generated

✅ **Successful Pipeline Run:**
- 40,000+ records processed
- Data quality score ≥ 85%
- All validation checks passed
- Reports generated successfully

---

**🎯 Result**: You now have a fully functional Airflow UI running on `http://localhost:8080` where you can monitor and manage the intelligent data platform's financial pipeline!
