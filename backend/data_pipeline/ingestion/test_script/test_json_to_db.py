#!/usr/bin/env python3
"""
JSON to Database Import Script
Uses JSON extractor methods to process JSON files and push data directly into database
"""

import os
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any

# Add parent directories to path for imports - system independent
current_dir = Path(__file__).parent.absolute()
project_root = current_dir.parent.parent.parent  # Go up to backend/
sys.path.insert(0, str(project_root))

try:
    from database.config.config import Config
    from database.db_connection import engine, Session
    from data_pipeline.ingestion.json_extractor import SECFactsExtractor
    from database.models.sec_facts_raw import BronzeSecFacts, BronzeSecFactsDict
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Project root: {project_root}")
    print("Make sure you're running from the correct directory and modules exist.")
    sys.exit(1)


def display_database_counts():
    """Display current record counts in database tables"""
    try:
        with Session() as session:
            # Count records in bronze_sec_facts table
            facts_count = session.query(BronzeSecFacts).count()

            # Count records in bronze_sec_facts_dict table
            dict_count = session.query(BronzeSecFactsDict).count()

            print("\n--- Database Record Counts ---")
            print(f"BronzeSecFacts (facts): {facts_count:,} records")
            print(f"BronzeSecFactsDict (dictionary): {dict_count:,} records")
            print(f"Total records: {facts_count + dict_count:,}")

            # Show sample CIK values if any records exist
            if facts_count > 0:
                sample_ciks = (
                    session.query(BronzeSecFacts.cik).distinct().limit(5).all()
                )
                cik_list = [cik[0] for cik in sample_ciks]
                print(f"Sample CIK values: {cik_list}")

            return facts_count, dict_count

    except Exception as e:
        print(f"Error querying database: {e}")
        return 0, 0


def process_json_file_to_database(json_file_path: str):
    """
    Process a single JSON file and push data directly to database

    Args:
        json_file_path: Path to the JSON file to process
    """
    json_file = Path(json_file_path)

    if not json_file.exists():
        print(f"JSON file not found: {json_file}")
        return False

    # Initialize extractor
    json_directory = json_file.parent
    db_config = Config()
    extractor = SECFactsExtractor(str(json_directory), db_config)

    print(f"Processing JSON file: {json_file.name}")

    try:
        # Process the JSON file directly to database
        result = extractor.process_single_file(json_file)

        if result["status"] == "success":
            print(f"Successfully processed {json_file.name}")
            print(f"CIK: {result['cik']}")
            print(f"Fact records: {result['fact_records']}")
            print(f"Dict records: {result['dict_records']}")
            print(f"Total records: {result['total_records']}")
            return True
        else:
            print(f"Failed to process {json_file.name}")
            print(f"Error: {result.get('error', 'Unknown error')}")
            return False

    except Exception as e:
        print(f"Error processing {json_file.name}: {e}")
        return False


def process_multiple_json_files(json_directory: str, file_pattern: str = "*.json"):
    """
    Process multiple JSON files in a directory

    Args:
        json_directory: Directory containing JSON files
        file_pattern: Pattern to match JSON files (default: "*.json")
    """
    json_dir = Path(json_directory)

    if not json_dir.exists():
        print(f"Directory not found: {json_directory}")
        return

    # Find all JSON files matching the pattern
    json_files = list(json_dir.glob(file_pattern))

    if not json_files:
        print(f"No JSON files found in {json_directory}")
        return

    print(f"Found {len(json_files)} JSON files to process")

    # Process each file
    successful = 0
    failed = 0

    for json_file in json_files:
        print(f"\n--- Processing {json_file.name} ---")
        if process_json_file_to_database(str(json_file)):
            successful += 1
        else:
            failed += 1

    print(f"\n--- Processing Complete ---")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total: {len(json_files)}")


def process_specific_cik(cik_number: str):
    """
    Process a specific CIK file

    Args:
        cik_number: CIK number (e.g., "0000001800")
    """
    json_directory = (
        "/Users/ssp/Documents/MS_CS/Projects_git/sp500_agentic_ai/data/company_facts"
    )
    json_file_path = os.path.join(json_directory, f"CIK{cik_number}.json")

    print(f"Processing CIK {cik_number}")
    return process_json_file_to_database(json_file_path)


def main():
    """Main function - process CIK0000001800.json and show database counts"""
    print("JSON to Database Import Script")
    print("=" * 40)

    # Show initial database counts
    print("\n--- Initial Database Counts ---")
    display_database_counts()

    # Process CIK0000001800.json
    print("\n--- Processing CIK0000001800.json ---")
    success = process_specific_cik("0000001800")

    # Show final database counts
    print("\n--- Final Database Counts ---")
    display_database_counts()

    if success:
        print("\nProcessing completed successfully!")
    else:
        print("\nProcessing failed!")


if __name__ == "__main__":
    main()
