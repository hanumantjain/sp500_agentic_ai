# SEC Company Facts JSON Processing Pipeline

This pipeline extracts and processes SEC company facts data from JSON files and loads it into TiDB using Apache Airflow for orchestration.

## Overview

The pipeline processes JSON files containing SEC company facts data from the `data/company_facts/` directory. Each JSON file contains financial data for a specific company identified by its CIK (Central Index Key).

## Architecture

```
data/company_facts/          # Source JSON files
├── CIK0000001800.json      # Company facts for CIK 1800
├── CIK0000002488.json      # Company facts for CIK 2488
└── ...                     # Additional company files

backend/data_pipeline/
├── ingestion/
│   └── json_extractor.py   # Core extraction logic
├── orchestration/
│   ├── dags/
│   │   └── sec_company_facts_dag.py  # Airflow DAG
│   ├── operators/
│   │   └── sec_facts_operators.py   # Custom operators
│   └── config/
│       └── pipeline_config.py       # Configuration
└── database/
    ├── config/
    │   └── config.py       # Database configuration
    └── models/
        └── sec_facts_raw.py # Database models
```

## Components

### 1. JSON Extractor (`json_extractor.py`)

The core component that:
- Scans the JSON directory for files
- Extracts CIK from filenames
- Parses JSON data and normalizes it
- Loads data into TiDB using SQLAlchemy

**Key Features:**
- Batch processing for performance
- Error handling and logging
- Data validation and type conversion
- Progress tracking

### 2. Airflow Operators (`sec_facts_operators.py`)

Custom operators for Airflow orchestration:

- **DatabaseHealthCheckOperator**: Verifies database connectivity
- **JSONFileScannerOperator**: Scans directory for JSON files
- **SECFactsExtractorOperator**: Processes JSON files and loads data
- **DataQualityCheckOperator**: Validates processing results
- **JSONFileProcessorOperator**: Processes individual files

### 3. Airflow DAG (`sec_company_facts_dag.py`)

The main orchestration workflow:

1. **Database Health Check** - Verify TiDB connectivity
2. **Scan JSON Files** - Discover available files
3. **Extract SEC Facts** - Process and load data
4. **Data Quality Check** - Validate results
5. **Generate Report** - Create processing summary
6. **Cleanup** - Optional cleanup tasks

### 4. Configuration (`pipeline_config.py`)

Centralized configuration for:
- File paths and directories
- Database connection settings
- Processing parameters
- Airflow settings
- Logging configuration

## Data Model

The pipeline creates two main tables in TiDB:

### BronzeSecFacts
Stores the actual financial data:
- `cik`: Company identifier
- `taxonomy`: Data taxonomy (e.g., 'us-gaap', 'dei')
- `tag`: Specific data tag
- `unit`: Unit of measurement
- `val`: Value
- `fy`: Fiscal year
- `fp`: Fiscal period
- `start_date`, `end_date`: Date range
- `frame`: Time frame reference
- `form`: SEC form type
- `filed`: Filing date
- `accn`: Accession number

### BronzeSecFactsDict
Stores metadata about data tags:
- `taxonomy`: Data taxonomy
- `tag`: Data tag
- `label`: Human-readable label
- `description`: Detailed description

## Usage

### Standalone Processing

Run the processing pipeline directly:

```bash
# Full pipeline
./backend/run_json_processing.sh

# Check only (no processing)
./backend/run_json_processing.sh --check-only

# Install dependencies only
./backend/run_json_processing.sh --install-only
```

### Airflow Orchestration

1. **Setup Airflow**:
   ```bash
   cd backend
   source sp500/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure Environment**:
   Create `.env` file with TiDB configuration:
   ```
   TIDB_HOST=your_tidb_host
   TIDB_PORT=4000
   TIDB_USER=your_username
   TIDB_PASSWORD=your_password
   TIDB_DB_NAME=your_database
   ```

3. **Start Airflow**:
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```

4. **Monitor DAG**:
   - Access Airflow UI at `http://localhost:8080`
   - Find the `sec_company_facts_json_processing` DAG
   - Enable and trigger the DAG

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TIDB_HOST` | TiDB host address | `127.0.0.1` |
| `TIDB_PORT` | TiDB port | `4000` |
| `TIDB_USER` | Database username | `root` |
| `TIDB_PASSWORD` | Database password | `` |
| `TIDB_DB_NAME` | Database name | `test` |
| `BATCH_SIZE` | Processing batch size | `1000` |
| `EXPECTED_MIN_FILES` | Minimum expected files | `500` |
| `EXPECTED_MIN_RECORDS` | Minimum expected records | `100000` |

### Processing Parameters

- **Batch Size**: Number of records processed per database transaction
- **Retry Logic**: Automatic retry on failures
- **Data Validation**: Quality checks for processed data
- **Error Handling**: Comprehensive error logging and reporting

## Monitoring and Logging

### Logs
- Processing logs: `backend/logs/sec_facts_processing.log`
- Airflow logs: Available in Airflow UI
- Processing reports: `data/normalised_data/processing_report_*.csv`

### Metrics
- Files processed
- Records loaded
- Success/failure rates
- Processing time
- Data quality metrics

## Troubleshooting

### Common Issues

1. **Database Connection Failed**:
   - Check TiDB service status
   - Verify connection parameters in `.env`
   - Test connection manually

2. **No JSON Files Found**:
   - Verify `data/company_facts/` directory exists
   - Check file permissions
   - Ensure files have `.json` extension

3. **Processing Failures**:
   - Check logs for specific error messages
   - Verify database schema exists
   - Check available disk space

4. **Airflow DAG Not Appearing**:
   - Verify DAG file is in correct location
   - Check for syntax errors in DAG file
   - Restart Airflow scheduler

### Performance Optimization

- Adjust `BATCH_SIZE` based on available memory
- Use database connection pooling
- Monitor TiDB performance metrics
- Consider parallel processing for large datasets

## Dependencies

See `requirements.txt` for complete dependency list:

- `apache-airflow==2.7.3`
- `SQLAlchemy==2.0.20`
- `PyMySQL==1.1.0`
- `pandas`
- `python-dotenv==1.0.0`

## Future Enhancements

- Parallel file processing
- Incremental processing
- Data quality dashboards
- Automated data validation
- Real-time monitoring alerts
- Data lineage tracking