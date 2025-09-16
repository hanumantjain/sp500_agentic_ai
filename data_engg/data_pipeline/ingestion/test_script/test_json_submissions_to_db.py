#!/usr/bin/env python3
"""
JSON Submissions to Database Import Script
Uses JSON extractor methods to process submissions JSON files and push data directly into database
"""

import os
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any

# Add parent directories to path for imports - system independent
current_dir = Path(__file__).parent.absolute()
project_root = current_dir.parent.parent.parent  # Go up to data_engg/
sys.path.insert(0, str(project_root))

try:
    from database.config.config import Config
    from database.db_connection import engine, Session
    from data_pipeline.ingestion.json_extractor_submissions import (
        SECSubmissionsExtractor,
    )
    from database.models.sec_submissions_raw import BronzeSecSubmissions
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Project root: {project_root}")
    print("Make sure you're running from the correct directory and modules exist.")
    sys.exit(1)


def display_database_counts():
    """Display current record counts in database tables"""
    try:
        with Session() as session:
            # Count records in bronze_sec_submissions table
            submissions_count = session.query(BronzeSecSubmissions).count()

            print("\n--- Database Record Counts ---")
            print(f"BronzeSecSubmissions (submissions): {submissions_count:,} records")

            # Show sample CIK values if any records exist
            if submissions_count > 0:
                sample_ciks = (
                    session.query(BronzeSecSubmissions.cik).distinct().limit(5).all()
                )
                cik_list = [cik[0] for cik in sample_ciks]
                print(f"Sample CIK values: {cik_list}")

                # Show sample forms
                sample_forms = (
                    session.query(BronzeSecSubmissions.form).distinct().limit(10).all()
                )
                form_list = [form[0] for form in sample_forms if form[0]]
                print(f"Sample forms: {form_list}")

            return submissions_count

    except Exception as e:
        print(f"Error querying database: {e}")
        return 0


def process_submissions_json_file_to_database(json_file_path: str):
    """
    Process a single submissions JSON file and push data directly to database

    Args:
        json_file_path: Path to the submissions JSON file to process
    """
    json_file = Path(json_file_path)

    if not json_file.exists():
        print(f"JSON file not found: {json_file}")
        return False

    # Initialize extractor
    extractor = SECSubmissionsExtractor(json_directory="", db_config=Config())

    print(f"Processing submissions JSON file: {json_file.name}")

    try:
        # Process the JSON file directly to database
        result = extractor.process_single_file(json_file)

        if result["status"] == "success":
            print(f"Successfully processed {json_file.name}")
            print(f"CIK: {result['cik']}")
            print(f"Submission records: {result['submission_records']}")
            print(f"Total records: {result['total_records']}")
            return True
        else:
            print(f"Failed to process {json_file.name}")
            print(f"Error: {result.get('error', 'Unknown error')}")
            return False

    except Exception as e:
        print(f"Error processing {json_file.name}: {e}")
        return False


def process_multiple_submissions_json_files(
    json_directory: str, file_pattern: str = "*.json"
):
    """
    Process multiple submissions JSON files in a directory

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
        if process_submissions_json_file_to_database(str(json_file)):
            successful += 1
        else:
            failed += 1

    print(f"\n--- Processing Complete ---")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total: {len(json_files)}")


def process_specific_submissions_cik(cik_number: str):
    """
    Process a specific CIK submissions file

    Args:
        cik_number: CIK number (e.g., "0000001800")
    """
    json_directory = "/Users/ssp/Documents/MS_CS/Projects_git/sp500_agentic_ai/data/submissions_facts"
    json_file_path = os.path.join(json_directory, f"CIK{cik_number}.json")

    print(f"Processing submissions CIK {cik_number}")
    return process_submissions_json_file_to_database(json_file_path)


def test_extractor_without_database():
    """
    Test the extractor to normalize data without inserting to database
    """
    print("\n--- Testing Extractor (No Database Insert) ---")

    json_file_path = "/Users/ssp/Documents/MS_CS/Projects_git/sp500_agentic_ai/data/submissions_facts/CIK0000001800.json"

    # Initialize extractor
    extractor = SECSubmissionsExtractor(json_directory="", db_config=Config())

    try:
        # Load and normalize the JSON data
        import json

        with open(json_file_path, "r") as f:
            json_data = json.load(f)

        # Extract CIK from filename
        cik = "0000001800"

        # Normalize the data
        normalized_records = extractor.normalize_submissions_data(json_data, cik)

        print(f"Normalized {len(normalized_records)} records")
        return True

    except Exception as e:
        print(f"Error testing extractor: {e}")
        import traceback

        traceback.print_exc()
        return False


def process_json_file(file_path):
    """Process a specific JSON file"""
    try:
        # Initialize extractor with required parameters
        json_directory = "../../../../data/submissions_facts/"
        extractor = SECSubmissionsExtractor(
            json_directory=json_directory, db_config=Config()
        )

        # Extract CIK from filename
        filename = os.path.basename(file_path)
        if filename.startswith("CIK") and filename.endswith(".json"):
            # Extract CIK from filename like CIK0000001800.json or CIK0000001800-submissions-001.json
            cik_part = filename.replace("CIK", "").replace(".json", "")
            if "-submissions-" in cik_part:
                cik = cik_part.split("-submissions-")[0]
            else:
                cik = cik_part
        else:
            print(f"Invalid filename format: {filename}")
            return False

        print(f"Processing submissions CIK {cik}")
        from pathlib import Path

        result = extractor.process_single_file(Path(file_path))
        return result.get("total_records", 0) > 0

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


def main():
    """Main function - process 10 specific JSON files and show database counts"""
    print("JSON Submissions to Database Import Script")
    print("=" * 50)

    # Test extractor first (without database)
    test_extractor_without_database()

    # Show initial database counts
    print("\n--- Initial Database Counts ---")
    display_database_counts()

    # List of specific JSON files to process (exactly as requested)
    json_files_to_process = [
        "../../../../data/submissions_facts/CIK0000001800-submissions-001.json",
        "../../../../data/submissions_facts/CIK0000001800-submissions-002.json",
        "../../../../data/submissions_facts/CIK0000001800.json",
        "../../../../data/submissions_facts/CIK0000002488-submissions-001.json",
        "../../../../data/submissions_facts/CIK0000002488-submissions-002.json",
        "../../../../data/submissions_facts/CIK0000002488.json",
        "../../../../data/submissions_facts/CIK0000002969-submissions-001.json",
        "../../../../data/submissions_facts/CIK0000002969.json",
        "../../../../data/submissions_facts/CIK0000004127-submissions-001.json",
        "../../../../data/submissions_facts/CIK0000004127.json",
    ]

    print(f"\n--- Processing {len(json_files_to_process)} JSON Files ---")

    successful_files = 0
    total_records_processed = 0

    for i, json_file_path in enumerate(json_files_to_process, 1):
        filename = os.path.basename(json_file_path)
        print(f"\n[{i}/{len(json_files_to_process)}] Processing {filename}...")

        try:
            success = process_json_file(json_file_path)
            if success:
                successful_files += 1
                print(f"✓ Successfully processed {filename}")
            else:
                print(f"✗ Failed to process {filename}")
        except Exception as e:
            print(f"✗ Error processing {filename}: {e}")
            continue

    # Show final database counts
    print("\n--- Final Database Counts ---")
    display_database_counts()

    print(f"\n--- Summary ---")
    print(f"Files attempted: {len(json_files_to_process)}")
    print(f"Files successful: {successful_files}")
    print(f"Files failed: {len(json_files_to_process) - successful_files}")

    if successful_files == len(json_files_to_process):
        print("\nAll files processed successfully!")
    elif successful_files > 0:
        print(
            f"\nPartially successful - {successful_files}/{len(json_files_to_process)} files processed!"
        )
    else:
        print("\nProcessing failed for all files!")


if __name__ == "__main__":
    main()
