#!/bin/bash

# SP500 Agentic AI - Airflow Startup Script
# This script starts the Apache Airflow webserver for the SP500 data pipeline

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}SP500 Agentic AI - Airflow Startup${NC}"
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

# Check if Airflow is initialized
if [ ! -f "$AIRFLOW_HOME/airflow.cfg" ]; then
    echo -e "${RED}Error: Airflow not initialized${NC}"
    echo -e "${YELLOW}Please run: airflow db init && airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin123${NC}"
    exit 1
fi

# Kill any existing Airflow processes
echo -e "${YELLOW}Checking for existing Airflow processes...${NC}"
EXISTING_PROCESSES=$(pgrep -f "airflow" || true)
if [ ! -z "$EXISTING_PROCESSES" ]; then
    echo -e "${YELLOW}Found existing Airflow processes. Killing them...${NC}"
    pkill -f "airflow" || true
    sleep 2
    echo -e "${GREEN}Existing Airflow processes terminated${NC}"
else
    echo -e "${GREEN}No existing Airflow processes found${NC}"
fi

# Start Airflow scheduler
echo -e "${GREEN}Starting Airflow scheduler...${NC}"
airflow scheduler &
SCHEDULER_PID=$!

# Start Airflow webserver
echo -e "${GREEN}Starting Airflow webserver on port 8080...${NC}"
echo -e "${BLUE}Airflow Home: $AIRFLOW_HOME${NC}"
echo -e "${BLUE}DAGs Folder: $AIRFLOW_HOME/dags${NC}"
echo ""

# Initialize database if needed (non-interactive)
echo -e "${YELLOW}Initializing Airflow database...${NC}"
echo "y" | airflow db init || true

# Start Airflow scheduler in background
echo -e "${GREEN}Starting Airflow scheduler...${NC}"
nohup airflow scheduler > scheduler.log 2>&1 &
SCHEDULER_PID=$!

# Wait a moment for scheduler to start
sleep 2

# Start webserver in background
nohup airflow webserver --port 8080 > webserver.log 2>&1 &
WEBSERVER_PID=$!

# Wait a moment for startup
sleep 3

# Check if webserver started successfully
if curl -s http://localhost:8080 > /dev/null 2>&1; then
    echo -e "${GREEN}Airflow webserver started successfully!${NC}"
    echo -e "${GREEN}Web UI: http://localhost:8080${NC}"
    echo -e "${GREEN}Username: admin${NC}"
    echo -e "${GREEN}Password: admin123${NC}"
    echo ""
    echo -e "${YELLOW}Available DAGs:${NC}"
    airflow dags list | grep -E "(sp500|sec|tutorial)" || echo "No project DAGs found"
    echo ""
    echo -e "${BLUE}To stop Airflow: kill $SCHEDULER_PID $WEBSERVER_PID${NC}"
    echo -e "${BLUE}Scheduler PID: $SCHEDULER_PID${NC}"
    echo -e "${BLUE}Webserver PID: $WEBSERVER_PID${NC}"
    echo -e "${BLUE}Logs: scheduler.log and webserver.log${NC}"
else
    echo -e "${RED}Failed to start Airflow webserver${NC}"
    echo -e "${YELLOW}Check the logs above for error details${NC}"
    exit 1
fi
