#!/bin/bash

# SP500 Agentic AI - Airflow Setup Script
# This script sets up Apache Airflow from scratch for the SP500 data pipeline

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}SP500 Agentic AI - Airflow Setup${NC}"
echo "================================================"

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORCHESTRATION_DIR="$SCRIPT_DIR/data_pipeline/orchestration"
VENV_DIR="$SCRIPT_DIR/sp500"

# Check if we're in the right directory
if [ ! -d "$ORCHESTRATION_DIR" ]; then
    echo -e "${RED}Error: orchestration directory not found at $ORCHESTRATION_DIR${NC}"
    exit 1
fi

# Check if virtual environment exists
if [ ! -f "$VENV_DIR/bin/activate" ]; then
    echo -e "${RED}Error: Virtual environment not found at $VENV_DIR${NC}"
    echo -e "${YELLOW}Please run: python -m venv sp500 && source sp500/bin/activate && pip install -r requirements.txt${NC}"
    exit 1
fi

# Navigate to orchestration directory
cd "$ORCHESTRATION_DIR"

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source "$VENV_DIR/bin/activate"

# Set Airflow home
export AIRFLOW_HOME="$(pwd)/airflow_config"

echo -e "${BLUE}Airflow Home: $AIRFLOW_HOME${NC}"

# Check if Airflow is already initialized
if [ -f "$AIRFLOW_HOME/airflow.cfg" ]; then
    echo -e "${YELLOW}Airflow is already initialized. Skipping initialization.${NC}"
else
    echo -e "${GREEN}Initializing Airflow database...${NC}"
    airflow db init
fi

# Set default password or use environment variable
AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD:-"admin123"}

# Check if admin user exists
echo -e "${GREEN}Checking for admin user...${NC}"
if airflow users list | grep -q "admin"; then
    echo -e "${YELLOW}Admin user already exists. Skipping user creation.${NC}"
    echo -e "${BLUE}To reset password, delete user first: airflow users delete --username admin${NC}"
else
    echo -e "${GREEN}Creating admin user...${NC}"
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@sp500-agentic.com \
        --password "$AIRFLOW_ADMIN_PASSWORD"
    echo -e "${GREEN}Admin user created with password: $AIRFLOW_ADMIN_PASSWORD${NC}"
fi

# Create dags directory if it doesn't exist
mkdir -p "$AIRFLOW_HOME/dags"

# Create a simple test DAG if it doesn't exist
TEST_DAG_FILE="$AIRFLOW_HOME/dags/sp500_test_dag.py"
if [ ! -f "$TEST_DAG_FILE" ]; then
    echo -e "${GREEN}Creating SP500 test DAG...${NC}"
    cat > "$TEST_DAG_FILE" << 'EOF'
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'ssp',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'sp500_data_pipeline_test',
    default_args=default_args,
    description='Test DAG for SP500 data pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['sp500', 'test', 'data-pipeline'],
)

# Define tasks
def test_python_task():
    """Test Python task"""
    print("SP500 Data Pipeline Test - Python Task Executed Successfully!")
    return "Python task completed"

# Bash task
bash_task = BashOperator(
    task_id='test_bash_task',
    bash_command='echo "SP500 Data Pipeline Test - Bash Task Executed Successfully!"',
    dag=dag,
)

# Python task
python_task = PythonOperator(
    task_id='test_python_task',
    python_callable=test_python_task,
    dag=dag,
)

# Set task dependencies
bash_task >> python_task
EOF
fi

# Verify DAGs are loaded
echo -e "${GREEN}Verifying DAGs...${NC}"
airflow dags list | grep -E "(sp500|tutorial)" || echo "No SP500 DAGs found"

echo ""
echo -e "${GREEN}Airflow setup completed successfully!${NC}"
echo -e "${BLUE}Next steps:${NC}"
echo -e "${BLUE}1. Run ./start_airflow.sh to start the webserver${NC}"
echo -e "${BLUE}2. Access the web UI at: http://localhost:8080${NC}"
echo -e "${BLUE}3. Login with: admin / admin123${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo -e "${YELLOW}- Airflow Home: $AIRFLOW_HOME${NC}"
echo -e "${YELLOW}- DAGs Folder: $AIRFLOW_HOME/dags${NC}"
echo -e "${YELLOW}- Database: $AIRFLOW_HOME/airflow.db${NC}"
