# Database

Database models, connections, and management utilities.

## Structure

```
database/
├── config/           # Database configuration
├── models/          # SQLAlchemy models
├── migrations/      # Alembic migrations
└── tests/          # Database tests
```

## Quick Start

```bash
# Setup database tables
./setup_database.sh

# Create specific table
python create_tables.py --create-table sp500_stooq_ohcl

# Drop table
python create_tables.py --drop-table sp500_stooq_ohcl

# Delete all data from table
python create_tables.py --delete-data sp500_stooq_ohcl
```

## Available Tables

| Table | Purpose |
|-------|---------|
| `sp500_stooq_ohcl` | S&P 500 OHLC stock data |
| `sp500_wik_list` | S&P 500 company information |
| `selected_changes_sp500` | S&P 500 component changes |
| `sp500_finnhub_news` | S&P 500 news data from Finnhub |
| `bronze_sec_facts` | Raw SEC facts data |
| `bronze_sec_submissions` | Raw SEC submissions data |

## Database Management

### Create Tables
```bash
python create_tables.py --create-all
python create_tables.py --create-table table_name
```

### Drop Tables
```bash
python create_tables.py --drop-all
python create_tables.py --drop-table table_name
```

### Query Data
```bash
python query_data.py --query "SELECT * FROM sp500_stooq_ohcl LIMIT 10"
python query_data.py --list-tables
```

## Configuration

Database connection settings in `config/config.py`:
- Environment variables: `.env.main`
- Connection: TiDB Cloud MySQL
- Models: SQLAlchemy ORM
