#!/bin/bash

# SP500 Agentic AI - Database Setup Script
# This script drops all existing tables and creates new ones using create_tables.py

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}SP500 Agentic AI - Database Setup${NC}"
echo "================================================"
echo -e "${YELLOW}This will DROP ALL existing tables and create new ones${NC}"
echo -e "${YELLOW}Tables to be created:${NC}"
echo "  - bronze_sec_facts"
echo "  - bronze_sec_facts_dict"
echo "  - bronze_sec_submissions"
echo "  - bronze_sec_submissions_raw"
echo "  - selected_changes_sp500"
echo "  - sp500_finnhub_news"
echo "  - sp500_stooq_ohcl"
echo "  - sp500_wik_list"
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

# Navigate to database directory
cd "$SCRIPT_DIR/database"

# Drop all existing tables and create new ones
echo -e "${YELLOW}Dropping all existing tables and creating new ones...${NC}"
echo "yes" | python3 create_tables.py --drop-all --create

if [ $? -eq 0 ]; then
    echo
    echo -e "${GREEN}Database setup completed successfully!${NC}"
    echo -e "${GREEN}All tables have been dropped and recreated${NC}"
else
    echo -e "${RED}Database setup failed!${NC}"
    exit 1
fi
