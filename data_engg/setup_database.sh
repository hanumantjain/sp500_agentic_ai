#!/bin/bash

# SP500 Agentic AI - Database Setup Script (Facts Tables)
# This script sets up the SEC facts database tables using Alembic migrations
# Tables: bronze_sec_facts, bronze_sec_facts_dict

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}SP500 Agentic AI - Database Setup (Facts)${NC}"
echo "================================================"
echo -e "${YELLOW}Setting up SEC Facts tables:${NC}"
echo "  - bronze_sec_facts"
echo "  - bronze_sec_facts_dict"
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

# Run the Python setup script for facts tables
echo -e "${YELLOW}Running facts database setup...${NC}"
python3 setup_database.py --type facts

if [ $? -eq 0 ]; then
    echo
    echo -e "${GREEN}Facts database setup completed successfully!${NC}"
    echo -e "${GREEN}Tables ready: bronze_sec_facts, bronze_sec_facts_dict${NC}"
else
    echo -e "${RED}Facts database setup failed!${NC}"
    exit 1
fi
