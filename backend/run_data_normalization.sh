#!/bin/bash

# Script to run the complete S&P 500 data normalization pipeline from backend directory
# This script extracts file names and normalizes all S&P 500 data with required columns
# Usage: ./run_data_normalization.sh [start_date] [end_date]
# Example: ./run_data_normalization.sh "2020-01-01" "2023-12-31"

echo "Starting S&P 500 Complete Data Pipeline..."
echo "=========================================="

# Navigate to the backend directory (where this script is located)
cd "$(dirname "$0")"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python3 is not installed or not in PATH"
    exit 1
fi

# Check if the Python script exists
if [ ! -f "data_pipeline/data_normalisation/raw_data_normalisation.py" ]; then
    echo "Error: raw_data_normalisation.py not found"
    exit 1
fi

# Check if input data directory exists
if [ ! -d "../data/sp500_ohcl" ]; then
    echo "Error: Input data directory '../data/sp500_ohcl' not found"
    echo "Please ensure the S&P 500 data files are in the correct location"
    exit 1
fi

echo "Running complete data pipeline..."
echo "Input directory: ../data/sp500_ohcl"
echo "Output directory: ../data/normalised_data"
echo "Processing: File extraction + Data normalization"
echo ""

# Run the Python script
# Get command line arguments for date filtering
START_DATE="$1"
END_DATE="$2"

python -c "
import sys
sys.path.append('data_pipeline')
from data_normalisation.raw_data_normalisation import DataPipeline

try:
    # Create pipeline instance with date filtering
    start_date = '$START_DATE' if '$START_DATE' else None
    end_date = '$END_DATE' if '$END_DATE' else None
    
    pipeline = DataPipeline(start_date=start_date, end_date=end_date)
    
    # Run complete pipeline
    results = pipeline.run_complete_pipeline()
    
    if results:
        print(f'Pipeline Results:')
        print(f'- File names extracted: {results[\"file_names_count\"]}')
        print(f'- Records normalized: {results[\"normalized_records\"]:,}')
        print(f'- Output files: {len(results[\"output_files\"])}')
    else:
        print('Pipeline failed!')
        sys.exit(1)
    
except Exception as e:
    print(f'Error: {e}')
    sys.exit(1)
"

# Check if the script ran successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "Data normalization completed successfully!"
    echo "Check the '../data/normalised_data' directory for output files"
else
    echo ""
    echo "Data normalization failed!"
    exit 1
fi
