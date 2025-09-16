#!/bin/bash

# SP500 Agentic AI - Database Setup Script (Submissions Tables)
# This script sets up the SEC submissions database tables using Alembic migrations
# Tables: bronze_sec_submissions

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}SP500 Agentic AI - Database Setup (Submissions)${NC}"
echo "================================================"
echo -e "${YELLOW}Setting up SEC Submissions tables:${NC}"
echo "  - bronze_sec_submissions"
echo
echo -e "${YELLOW}Table Schema:${NC}"
echo "  - Composite Primary Key: (cik, accession_number, filing_date, acceptance_datetime)"
echo "  - 15 data columns + 2 audit columns (created_at, updated_at)"
echo

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/sp500"

# Check if virtual environment exists
if [ ! -f "$VENV_DIR/bin/activate" ]; then
    echo -e "${RED}Error: Virtual environment not found at $VENV_DIR${NC}"
    echo -e "${YELLOW}Please run: python -m venv sp500 && source sp500/bin/activate && pip install -r requirements.txt${NC}"
    exit 1
fi

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source "$VENV_DIR/bin/activate"

# Run the Python setup script for submissions tables
echo -e "${YELLOW}Running submissions database setup...${NC}"
python3 setup_database.py --type submissions

if [ $? -eq 0 ]; then
    echo
    echo -e "${GREEN}Submissions database setup completed successfully!${NC}"
    echo -e "${GREEN}Table ready: bronze_sec_submissions${NC}"
    echo
    echo -e "${BLUE}Next steps:${NC}"
    echo "1. Use json_extractor_submissions.py to process SEC submissions JSON files"
    echo "2. Run your Airflow DAG for submissions data ingestion"
    echo "3. Monitor the data pipeline logs"
else
    echo -e "${RED}Submissions database setup failed!${NC}"
    exit 1
fi