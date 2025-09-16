#!/bin/bash

# Script to process all CIK files from data/submissionCIK directory
# using test_single_cik.py

# Set the directories
INPUT_DIR="../data/submissionCIK"
OUTPUT_DIR="../data/submissions_sec_docs"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Change to the script directory
cd "$SCRIPT_DIR"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Input directory $INPUT_DIR does not exist!"
    exit 1
fi

# Count total files
TOTAL_FILES=$(find "$INPUT_DIR" -name "test_CIK*.csv" | wc -l)
echo "Found $TOTAL_FILES CIK files to process"
echo "=========================================="

# Counter for progress
CURRENT=0

# Process each CIK file
for input_file in "$INPUT_DIR"/test_CIK*.csv; do
    # Check if file exists (in case no files match the pattern)
    if [ ! -f "$input_file" ]; then
        echo "No CIK files found in $INPUT_DIR"
        exit 1
    fi

    # Extract CIK from filename (e.g., test_CIK0000001800.csv -> CIK0000001800)
    filename=$(basename "$input_file")
    cik_name=$(echo "$filename" | sed 's/test_\(.*\)\.csv/\1/')

    # Set output file name
    output_file="$OUTPUT_DIR/${cik_name}_docs.csv"

    # Increment counter
    CURRENT=$((CURRENT + 1))

    echo ""
    echo "[$CURRENT/$TOTAL_FILES] Processing $cik_name"
    echo "Input:  $input_file"
    echo "Output: $output_file"
    echo "----------------------------------------"

    # Run the Python script
    python test_single_cik.py --input "$input_file" --docs-out "$output_file"

    # Check if the script ran successfully
    if [ $? -eq 0 ]; then
        echo "✓ Successfully processed $cik_name"
    else
        echo "✗ Failed to process $cik_name"
    fi

    echo "----------------------------------------"
done

echo ""
echo "=========================================="
echo "Processing completed!"
echo "Processed $CURRENT CIK files"
echo "Output files saved to: $OUTPUT_DIR"
echo "=========================================="
