# Data Engineering Pipeline

S&P 500 data processing and ingestion pipeline.

## Quick Start

### Navigate to data_engg directory
```bash
cd data_engg
```

### Setup environment
```bash
python -m venv sp500
source sp500/bin/activate
pip install -r requirements.txt
```

### Make shell scripts executable
```bash
chmod +x *.sh
```

### Setup database
```bash
./setup_database.sh
```

### Start Airflow
```bash
./setup_airflow.sh
./start_airflow.sh
```

### Access Airflow Web UI
- **URL**: http://localhost:8080
- **Login**: admin / admin123

### Available DAGs to trigger
- `sp500_data_pipeline_test` (test DAG)
- `sec_json_ingestion` (SEC company facts processing)
- `sec_submissions_ingestion` (SEC submissions processing)

### Run all data ingestion
```bash
./run_all_ingestion.sh
```
**Populates 5 tables:**
- `selected_changes_sp500`: S&P 500 component changes data
- `sp500_finnhub_news`: S&P 500 news data from Finnhub API
- `sp500_stooq_ohcl`: S&P 500 OHLC stock price data
- `sp500_wik_list`: S&P 500 company information and metadata
- `sp500_corporate_actions`: S&P 500 corporate actions data (dividends, splits, etc.)

```bash
cd data_engg
```

## Shell Scripts

| Script | Purpose |
|--------|---------|
| `run_data_normalization_sp500.sh` | Normalize S&P 500 OHLC data |
| `run_data_normalisation_company_facts.sh` | Process company facts data |
| `run_data_normalisation_submissions_facts.sh` | Process SEC submissions |
| `run_all_ingestion.sh` | Run all S&P 500 data ingestion scripts |
| `setup_database.sh` | Drop all tables and create new ones |
| `setup_airflow.sh` | Configure Airflow environment |
| `start_airflow.sh` | Start Airflow webserver |

## Directory Structure

- `data_pipeline/` - Data processing scripts
- `database/` - Database models and management
- `requirements.txt` - Python dependencies

## Documentation

- [Data Pipeline](data_pipeline/README.md)
- [Database](database/README.md)
- [Orchestration](data_pipeline/orchestration/README.md) - Apache Airflow setup and DAG management