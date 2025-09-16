# SP500 Data Pipeline Orchestration

Apache Airflow orchestration for the SP500 Agentic AI data pipeline.

## Quick Start

### Option 1: Startup Script (Recommended)
```bash
# From data_engg directory
./start_airflow.sh
```

### Option 2: Manual Setup
```bash
cd data_engg/data_pipeline/orchestration
source ../../sp500/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow_config
airflow webserver --port 8080
```

## Web UI Access
- **URL**: http://localhost:8080
- **Login**: admin / admin123

## Available DAGs

| DAG | Description | Schedule | Tasks |
|-----|-------------|----------|-------|
| `sp500_data_pipeline_test` | Test DAG for setup verification | Daily | test_bash_task, test_python_task |
| `sec_json_ingestion` | SEC company facts JSON processing | Manual | scan_json_files, process_all_json_files |
| `sec_submissions_ingestion` | SEC submissions data processing | Manual | validate_submissions_setup, scan_submissions_files, process_all_submissions_files |

## Directory Structure
```
orchestration/
├── airflow_config/
│   ├── airflow.cfg          # Main configuration
│   ├── airflow.db           # SQLite database
│   ├── dags/                # DAG files
│   │   ├── sp500_test_dag.py
│   │   ├── sec_json_ingestion_dag.py
│   │   └── sec_submissions_ingestion_dag.py
│   └── logs/                # Execution logs
└── README.md
```

## Common Commands

### DAG Management
```bash
airflow dags list                                    # List all DAGs
airflow dags test sp500_data_pipeline_test 2025-09-13  # Test DAG
airflow dags trigger sp500_data_pipeline_test        # Trigger DAG run
```

### Task Management
```bash
airflow tasks list sp500_data_pipeline_test          # List tasks
airflow tasks test sp500_data_pipeline_test test_bash_task 2025-09-13  # Test task
```

### Database Management
```bash
airflow db init                                      # Initialize database
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin123
```

## Configuration
- **Database**: SQLite (airflow.db)
- **Webserver Port**: 8080
- **DAGs Folder**: `airflow_config/dags/`
- **Logs Folder**: `airflow_config/logs/`
- **Environment**: `AIRFLOW_HOME=$(pwd)/airflow_config`

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "No module named 'airflow'" | Activate virtual environment: `source ../../sp500/bin/activate` |
| "AIRFLOW_HOME not set" | Set environment: `export AIRFLOW_HOME=$(pwd)/airflow_config` |
| "DAG not showing up" | Check syntax: `python airflow_config/dags/your_dag.py` |
| "Port 8080 already in use" | Kill process: `pkill -f "airflow webserver"` or use port 8081 |

