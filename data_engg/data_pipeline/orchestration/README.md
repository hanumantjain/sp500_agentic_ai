# SP500 Data Pipeline Orchestration

This directory contains the Apache Airflow orchestration setup for the SP500 Agentic AI data pipeline.

## Directory Structure

```
orchestration/
├── airflow_config/           # Airflow configuration and database
│   ├── airflow.cfg          # Main Airflow configuration file
│   ├── airflow.db           # SQLite database (created during init)
│   ├── dags/                # DAG files directory
│   │   └── sp500_test_dag.py # SP500 test DAG
│   └── logs/                # Airflow execution logs
├── config/                   # Pipeline configuration files
├── operators/               # Custom Airflow operators
├── sensors/                 # Custom Airflow sensors
└── README.md               # This file
```

## Quick Start

### Option 1: Using the Startup Script (Recommended)
```bash
# From the backend directory
./start_airflow.sh
```

### Option 2: Manual Setup
```bash
# Navigate to orchestration directory
cd backend/data_pipeline/orchestration

# Activate virtual environment
source ../../sp500/bin/activate

# Set Airflow home
export AIRFLOW_HOME=$(pwd)/airflow_config

# Start webserver
airflow webserver --port 8080
```

## Accessing the Web UI

1. Open your browser and go to: http://localhost:8080
2. Login credentials:
   - Username: `admin`
   - Password: `admin`

## Available DAGs

### SP500 Test DAG (`sp500_data_pipeline_test`)
- Description: Simple test DAG to verify Airflow setup
- Schedule: Daily (runs every day)
- Tasks:
  - `test_bash_task`: Executes a bash command
  - `test_python_task`: Executes a Python function
- Owner: `ssp`

## Configuration

### Airflow Configuration
- Database: SQLite (airflow.db)
- Webserver Port: 8080
- DAGs Folder: `airflow_config/dags/`
- Logs Folder: `airflow_config/logs/`

### Environment Variables
- `AIRFLOW_HOME`: Set to `$(pwd)/airflow_config`

## DAG Development

### Creating New DAGs
1. Create Python files in `airflow_config/dags/`
2. Follow Airflow DAG best practices
3. Test DAGs using `airflow dags list` and `airflow dags test`

### Example DAG Structure
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'ssp',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 13),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'your_dag_name',
    default_args=default_args,
    description='Your DAG description',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['sp500', 'data-pipeline'],
)

# Define your tasks here
```

## Common Commands

### DAG Management
```bash
# List all DAGs
airflow dags list

# Test a DAG
airflow dags test sp500_data_pipeline_test 2025-09-13

# Trigger a DAG run
airflow dags trigger sp500_data_pipeline_test
```

### Task Management
```bash
# List tasks in a DAG
airflow tasks list sp500_data_pipeline_test

# Test a specific task
airflow tasks test sp500_data_pipeline_test test_bash_task 2025-09-13
```

### Database Management
```bash
# Initialize database (if needed)
airflow db init

# Create admin user (if needed)
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
```

## Troubleshooting

### Common Issues

1. "No module named 'airflow'"
   - Ensure virtual environment is activated: `source ../../sp500/bin/activate`

2. "AIRFLOW_HOME not set"
   - Set environment variable: `export AIRFLOW_HOME=$(pwd)/airflow_config`

3. "DAG not showing up"
   - Check DAG syntax: `python airflow_config/dags/your_dag.py`
   - Ensure DAG is in correct folder: `airflow_config/dags/`

4. "Port 8080 already in use"
   - Kill existing process: `pkill -f "airflow webserver"`
   - Or use different port: `airflow webserver --port 8081`

