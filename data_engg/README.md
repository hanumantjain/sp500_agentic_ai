# Data Engineering Pipeline

S&P 500 data processing and ingestion pipeline.

## Quick Start

```bash
# Setup environment
python -m venv sp500
source sp500/bin/activate
pip install -r requirements.txt

# Run data normalization
./run_data_normalization_sp500.sh

# Setup database
./setup_database.sh

# Start Airflow
./start_airflow.sh
```

## Shell Scripts

| Script | Purpose |
|--------|---------|
| `run_data_normalization_sp500.sh` | Normalize S&P 500 OHLC data |
| `run_data_normalisation_company_facts.sh` | Process company facts data |
| `run_data_normalisation_submissions_facts.sh` | Process SEC submissions |
| `setup_database.sh` | Initialize database tables |
| `setup_database_submissions.sh` | Setup submissions tables |
| `setup_airflow.sh` | Configure Airflow environment |
| `start_airflow.sh` | Start Airflow webserver |

## Directory Structure

- `data_pipeline/` - Data processing scripts
- `database/` - Database models and management
- `requirements.txt` - Python dependencies

## Documentation

- [Data Pipeline](data_pipeline/README.md)
- [Database](database/README.md)