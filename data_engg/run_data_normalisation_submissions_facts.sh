#!/bin/bash

# Script to run the CIK Submissions Extractor for submissions facts data
# This script copies CIK submission files from a specified folder to the output directory
# Usage: ./run_data_normalisation_submissions_facts.sh [data_source_folder_path] [batch_size]
# Example: ./run_data_normalisation_submissions_facts.sh "/path/to/submissions/data" 5000

# Make this script executable
chmod +x "$0"

echo "Starting CIK Submissions File Copy Pipeline..."
echo "============================================="

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

# Get command line arguments
DATA_SOURCE_FOLDER="$1"
BATCH_SIZE="${2:-5000}"  # Default to 5000 if not provided

# Check if data source folder path is provided
if [ -z "$DATA_SOURCE_FOLDER" ]; then
    echo "Error: Data source folder path is required"
    echo "Usage: ./run_data_normalisation_submissions_facts.sh \"<data_folder_path>\" [batch_size]"
    echo "Example: ./run_data_normalisation_submissions_facts.sh \"/path/to/submissions/data\" 5000"
    exit 1
fi

echo "Configuration:"
echo "- S&P 500 CSV: ../data/S_and_P_500_component_stocks.csv"
echo "- Data Source Folder: $DATA_SOURCE_FOLDER"
echo "- Output Directory: ../data/submissions_facts"
echo "- Batch Size: $BATCH_SIZE files per batch"
echo ""

# Check if data source folder exists
if [ ! -d "$DATA_SOURCE_FOLDER" ]; then
    echo "Error: Data source folder '$DATA_SOURCE_FOLDER' not found"
    echo "Please provide a valid path to the folder containing CIK submission files"
    exit 1
fi

echo "Running CIK submissions file copy operation..."
echo ""

# Run the Python script
python3 -c "
import sys
sys.path.append('data_pipeline')
from data_normalisation.raw_data_normalisation_company_facts import CIKsubmissionsExtract

try:
    # Create extractor instance
    extractor = CIKsubmissionsExtract(
        data_source_folder='$DATA_SOURCE_FOLDER',
        batch_size=$BATCH_SIZE
    )
    
    print('Step 1: Loading CIK values from S&P 500 CSV...')
    cik_values = extractor.load_cik_values()
    print(f'Loaded {len(cik_values)} CIK values')
    print('')
    
    print('Step 2: Copying matched CIK submission files...')
    copied_files = extractor.copy_matched_files()
    print(f'Copy operation completed')
    print(f'Copied {len(copied_files)} CIK submission files to submissions_facts directory')
    print('')
    
    print('CIK Submissions File Copy Pipeline completed successfully!')
    
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"

# Check if the script ran successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "CIK submissions file copy completed successfully!"
    echo "Check the '../data/submissions_facts' directory for copied files"
    echo ""
    echo "Files are ready for further processing!"
else
    echo ""
    echo "CIK submissions file copy failed!"
    exit 1
fi
