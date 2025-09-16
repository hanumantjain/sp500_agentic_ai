#!/bin/bash

# Script to run the CIK File Extractor for company facts data
# This script copies CIK files from a specified folder to the output directory
# Usage: ./run_data_normalisation_company_facts.sh [data_source_folder_path]
# Example: ./run_data_normalisation_company_facts.sh "/path/to/company/facts/data"

# Make this script executable
chmod +x "$0"

echo "Starting CIK File Copy Pipeline..."
echo "=================================="

# Navigate to the data_engg directory (where this script is located)
cd "$(dirname "$0")"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python3 is not installed or not in PATH"
    exit 1
fi

# Check if the Python script exists
if [ ! -f "data_pipeline/data_normalisation/raw_data_normalisation_company_facts.py" ]; then
    echo "Error: raw_data_normalisation_company_facts.py not found"
    exit 1
fi

# Check if S&P 500 CSV file exists
if [ ! -f "../data/S_and_P_500_component_stocks.csv" ]; then
    echo "Error: S&P 500 component stocks CSV file not found"
    echo "Please ensure '../data/S_and_P_500_component_stocks.csv' exists"
    exit 1
fi

# Get command line argument for data source folder
DATA_SOURCE_FOLDER="$1"

# Check if data source folder path is provided
if [ -z "$DATA_SOURCE_FOLDER" ]; then
    echo "Error: Data source folder path is required"
    echo "Usage: ./run_data_normalisation_company_facts.sh \"<data_folder_path>\""
    echo "Example: ./run_data_normalisation_company_facts.sh \"/path/to/company/facts/data\""
    exit 1
fi

echo "Configuration:"
echo "- S&P 500 CSV: ../data/S_and_P_500_component_stocks.csv"
echo "- Data Source Folder: $DATA_SOURCE_FOLDER"
echo "- Output Directory: ../data/normalised_data/company_facts"
echo ""

# Check if data source folder exists
if [ ! -d "$DATA_SOURCE_FOLDER" ]; then
    echo "Error: Data source folder '$DATA_SOURCE_FOLDER' not found"
    echo "Please provide a valid path to the folder containing CIK files"
    exit 1
fi

echo "Running CIK file copy operation..."
echo ""

# Run the Python script
python3 -c "
import sys
sys.path.append('data_pipeline')
from data_normalisation.raw_data_normalisation_company_facts import CIKFileExtractor

try:
    # Create extractor instance
    extractor = CIKFileExtractor(
        data_source_folder='$DATA_SOURCE_FOLDER'
    )
    
    print('Step 1: Loading CIK values from S&P 500 CSV...')
    cik_values = extractor.load_cik_values()
    print(f'Loaded {len(cik_values)} CIK values')
    print('')
    
    print('Step 2: Copying matched CIK files...')
    copied_files = extractor.copy_matched_files()
    print(f'Copy operation completed')
    print(f'Copied {len(copied_files)} CIK files to company_facts directory')
    print('')
    
    print('CIK File Copy Pipeline completed successfully!')
    
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"

# Check if the script ran successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "CIK file copy completed successfully!"
    echo "Check the '../data/normalised_data/company_facts' directory for copied files"
    echo ""
    echo "Files are ready for further processing!"
else
    echo ""
    echo "CIK file copy failed!"
    exit 1
fi