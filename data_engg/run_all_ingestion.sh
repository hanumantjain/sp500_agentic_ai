#!/bin/bash

# SP500 Agentic AI - Run All Data Ingestion Scripts
# This script runs all S&P 500 data ingestion scripts

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}SP500 Agentic AI - Data Ingestion Pipeline${NC}"
echo "================================================"

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INGESTION_DIR="$SCRIPT_DIR/data_pipeline/ingestion"

# Check if we're in the right directory
if [ ! -d "$INGESTION_DIR" ]; then
    echo -e "${RED}Error: ingestion directory not found at $INGESTION_DIR${NC}"
    exit 1
fi

# Navigate to ingestion directory
cd "$INGESTION_DIR"

echo -e "${YELLOW}Running S&P 500 data ingestion scripts...${NC}"
echo ""

# 1. S&P 500 Component Changes
echo -e "${BLUE}1. S&P 500 Component Changes Ingestion${NC}"
echo "----------------------------------------"
python sp500_component_changes_ingestion.py
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Component Changes ingestion completed${NC}"
else
    echo -e "${RED}✗ Component Changes ingestion failed${NC}"
    exit 1
fi
echo ""

# 2. S&P 500 Finnhub News
echo -e "${BLUE}2. S&P 500 Finnhub News Ingestion${NC}"
echo "----------------------------------------"
python sp500_finnhub_news_ingestion.py
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Finnhub News ingestion completed${NC}"
else
    echo -e "${RED}✗ Finnhub News ingestion failed${NC}"
    exit 1
fi
echo ""

# 3. S&P 500 Stooq OHLC
echo -e "${BLUE}3. S&P 500 Stooq OHLC Ingestion${NC}"
echo "----------------------------------------"
python sp500_stooq_ohcl_ingestion.py
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Stooq OHLC ingestion completed${NC}"
else
    echo -e "${RED}✗ Stooq OHLC ingestion failed${NC}"
    exit 1
fi
echo ""

# 4. S&P 500 Wiki List
echo -e "${BLUE}4. S&P 500 Wiki List Ingestion${NC}"
echo "----------------------------------------"
python sp500_wik_list_ingestion.py
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Wiki List ingestion completed${NC}"
else
    echo -e "${RED}✗ Wiki List ingestion failed${NC}"
    exit 1
fi
echo ""

echo -e "${GREEN}All S&P 500 data ingestion completed successfully!${NC}"
echo -e "${BLUE}Summary:${NC}"
echo -e "${BLUE}- Component Changes: ✓${NC}"
echo -e "${BLUE}- Finnhub News: ✓${NC}"
echo -e "${BLUE}- Stooq OHLC: ✓${NC}"
echo -e "${BLUE}- Wiki List: ✓${NC}"
