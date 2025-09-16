# Data Pipeline

S&P 500 data processing and normalization pipeline.

## Structure

```
data_pipeline/
├── data_normalisation/     # Data cleaning and normalization
├── ingestion/             # Database ingestion scripts
└── orchestration/         # Airflow DAGs
```

## Data Normalization

### S&P 500 OHLC Data
```bash
# From data_engg directory
./run_data_normalization_sp500.sh [start_date] [end_date]

# Example
./run_data_normalization_sp500.sh "2020-01-01" "2023-12-31"
```

### Company Facts
```bash
./run_data_normalisation_company_facts.sh
```

### SEC Submissions
```bash
./run_data_normalisation_submissions_facts.sh
```

## Data Ingestion

### S&P 500 Tables
```bash
cd data_pipeline/ingestion

# OHLC data
python sp500_stooq_ohcl_ingestion.py --input-file ../../data/normalised_data/normalized_sp500_data.csv

# Wiki list
python sp500_wik_list_ingestion.py --input-file ../../data/S_and_P_500_component_stocks.csv

# Component changes
python sp500_component_changes_ingestion.py --input-file ../../data/selected_changes_s_and_p_component_stocks.csv
```

## Orchestration

Airflow DAGs for automated data processing:
- `sec_json_ingestion_dag.py` - SEC facts ingestion
- `sec_submissions_ingestion_dag.py` - SEC submissions processing
- `sp500_test_dag.py` - Test DAG

## Input/Output

- **Input**: `../data/sp500_stooq_ohcl/`, `../data/company_facts/`, `../data/submissions_facts/`
- **Output**: `../data/normalised_data/`
- **Database**: Tables created via `database/create_tables.py`