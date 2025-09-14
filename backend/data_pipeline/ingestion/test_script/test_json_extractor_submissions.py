"""
Test script for JSON Submissions Extractor
Tests the process_single_file method and converts JSON data to CSV for verification
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
    from database.db_connection import engine
    from data_pipeline.ingestion.json_extractor_submissions import (
        SECSubmissionsExtractor,
    )
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Project root: {project_root}")
    print("Make sure you're running from the correct directory and modules exist.")
    sys.exit(1)


class JSONToCSVConverter:
    """Converts JSON submissions data to CSV format for verification"""

    def __init__(self, output_dir: str = "../test_output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def convert_submissions_to_csv(
        self, normalized_records: List[tuple], cik: str
    ) -> str:
        """
        Convert normalized submissions data to a single CSV format

        Args:
            normalized_records: List of (record_type, record_data) tuples
            cik: Company CIK identifier

        Returns:
            Path to created CSV file
        """
        # Extract only the filing records (should be all records now)
        filing_records = []

        for record_type, record_data in normalized_records:
            if record_type == "filing":
                filing_records.append(record_data)

        # Create single CSV file
        if filing_records:
            df = pd.DataFrame(filing_records)
            csv_path = self.output_dir / f"submissions_{cik}_001.csv"
            df.to_csv(csv_path, index=False)
            return str(csv_path)

        return None


def test_single_file_processing():
    """Test the process_single_file method with a sample JSON file"""

    # Configuration
    json_directory = "/Users/ssp/Documents/MS_CS/Projects_git/sp500_agentic_ai/data/submissions_facts"
    db_config = Config()

    # Initialize extractor and CSV converter
    extractor = SECSubmissionsExtractor(json_directory, db_config)
    csv_converter = JSONToCSVConverter()

    # Use specific test file - submissions-001
    test_file = Path(
        "/Users/ssp/Documents/MS_CS/Projects_git/sp500_agentic_ai/data/submissions_facts/CIK0000001800-submissions-001.json"
    )

    if not test_file.exists():
        print(f"Test file not found: {test_file}")
        return

    print(f"Testing with file: {test_file.name}")

    try:
        # Process the JSON file and extract data
        result = extractor.process_single_file(test_file)
        print(f"Processing result: {result}")

        # Extract and normalize data for CSV conversion
        cik = extractor.extract_cik_from_filename(test_file)
        json_data = extractor.load_json_file(test_file)
        normalized_records = extractor.normalize_submissions_data(json_data, cik)

        # Convert normalized data to single CSV file
        csv_file = csv_converter.convert_submissions_to_csv(normalized_records, cik)

        # Count records by type
        record_counts = {}
        for record_type, _ in normalized_records:
            record_counts[record_type] = record_counts.get(record_type, 0) + 1

        print(f"Successfully processed {test_file.name}")
        print(f"Created CSV file: {csv_file}")
        print(f"Records processed: {len(normalized_records)} total")

        # Print detailed record counts
        for record_type, count in record_counts.items():
            print(f"  {record_type}: {count} records")

        # Report database operation status
        if result["status"] == "success":
            print("Data processing successful (database save not implemented yet)")
        else:
            print(f"Processing failed: {result.get('error', 'Unknown error')}")
            print("CSV file created for verification despite processing error")

        # Display sample data from the CSV file
        print("\n" + "=" * 50)
        print("SAMPLE DATA FROM CSV FILE:")
        print("=" * 50)

        if csv_file:
            print(f"\nFile: {Path(csv_file).name}")
            try:
                df = pd.read_csv(csv_file)
                print(f"Shape: {df.shape}")
                print("Columns:", list(df.columns))
                if not df.empty:
                    print("Sample rows:")
                    print(df.head(5).to_string())
                else:
                    print("No data in file")
            except Exception as e:
                print(f"Error reading CSV file: {e}")

    except Exception as e:
        print(f"Error during testing: {str(e)}")
        raise


def test_data_extraction():
    """Test the data extraction and normalization logic"""

    # Configuration
    json_directory = "/Users/ssp/Documents/MS_CS/Projects_git/sp500_agentic_ai/data/submissions_facts"
    db_config = Config()

    # Initialize extractor
    extractor = SECSubmissionsExtractor(json_directory, db_config)

    # Test file - submissions-001
    test_file = Path(
        "/Users/ssp/Documents/MS_CS/Projects_git/sp500_agentic_ai/data/submissions_facts/CIK0000001800-submissions-001.json"
    )

    if not test_file.exists():
        print(f"Test file not found: {test_file}")
        return

    print(f"Testing data extraction with: {test_file.name}")

    try:
        # Load and extract data
        cik = extractor.extract_cik_from_filename(test_file)
        json_data = extractor.load_json_file(test_file)

        print(f"Extracted CIK: {cik}")
        print(f"JSON data keys: {list(json_data.keys())}")

        # Test normalization
        normalized_records = extractor.normalize_submissions_data(json_data, cik)

        print(f"Normalized {len(normalized_records)} records")

        # Count by type
        type_counts = {}
        for record_type, _ in normalized_records:
            type_counts[record_type] = type_counts.get(record_type, 0) + 1

        print("Record type distribution:")
        for record_type, count in type_counts.items():
            print(f"  {record_type}: {count}")

        # Show sample of each type
        print("\nSample records by type:")
        shown_types = set()
        for record_type, record_data in normalized_records:
            if record_type not in shown_types:
                print(f"\n{record_type.upper()} sample:")
                print(f"  Keys: {list(record_data.keys())}")
                # Show first few key-value pairs
                sample_items = list(record_data.items())[:5]
                for key, value in sample_items:
                    print(f"  {key}: {value}")
                shown_types.add(record_type)

    except Exception as e:
        print(f"Error during data extraction test: {str(e)}")
        raise


def test_both_files():
    """Test both main file and submissions-001 file for comparison"""

    # Configuration
    json_directory = "/Users/ssp/Documents/MS_CS/Projects_git/sp500_agentic_ai/data/submissions_facts"
    db_config = Config()

    # Initialize extractor and CSV converter
    extractor = SECSubmissionsExtractor(json_directory, db_config)
    csv_converter = JSONToCSVConverter()

    # Test files
    main_file = Path(json_directory + "/CIK0000001800.json")
    sub001_file = Path(json_directory + "/CIK0000001800-submissions-001.json")

    files_to_test = [
        (main_file, "main", "CIK0000001800"),
        (sub001_file, "submissions-001", "CIK0000001800_001"),
    ]

    for test_file, file_type, csv_suffix in files_to_test:
        print(f"\n{'='*60}")
        print(f"TESTING {file_type.upper()}: {test_file.name}")
        print(f"{'='*60}")

        if not test_file.exists():
            print(f"Test file not found: {test_file}")
            continue

        try:
            # Extract and normalize data
            cik = extractor.extract_cik_from_filename(test_file)
            json_data = extractor.load_json_file(test_file)

            print(f"Extracted CIK: {cik}")
            print(f"JSON data keys: {list(json_data.keys())}")

            # Check the structure
            if "filings" in json_data:
                print(
                    f"Has 'filings' key with keys: {list(json_data['filings'].keys())}"
                )
                if "recent" in json_data["filings"]:
                    recent_keys = list(json_data["filings"]["recent"].keys())
                    print(f"filings.recent keys: {recent_keys}")
                    print(
                        f"Number of records: {len(json_data['filings']['recent'].get('filingDate', []))}"
                    )
            else:
                print("Direct structure (no 'filings' key)")
                print(f"Number of records: {len(json_data.get('filingDate', []))}")

            # Test normalization
            normalized_records = extractor.normalize_submissions_data(json_data, cik)
            print(f"Normalized {len(normalized_records)} records")

            # Count by type
            type_counts = {}
            for record_type, _ in normalized_records:
                type_counts[record_type] = type_counts.get(record_type, 0) + 1

            print("Record type distribution:")
            for record_type, count in type_counts.items():
                print(f"  {record_type}: {count}")

            # Convert to CSV
            csv_file = csv_converter.convert_submissions_to_csv(
                normalized_records, csv_suffix
            )

            if csv_file:
                print(f"Created CSV file: {csv_file}")

                # Show CSV info
                try:
                    df = pd.read_csv(csv_file)
                    print(f"CSV Shape: {df.shape}")
                    print(f"CSV Columns: {list(df.columns)}")
                    if not df.empty:
                        print("First 3 rows:")
                        print(df.head(3).to_string())
                except Exception as e:
                    print(f"Error reading CSV: {e}")
            else:
                print("No CSV file created (no records)")

        except Exception as e:
            print(f"Error processing {file_type}: {e}")
            import traceback

            traceback.print_exc()


def main():
    """Main test function"""
    print("Starting JSON Submissions Extractor Comparison Test")
    print("Will test both main file and submissions-001 file")

    try:
        # Test both files for comparison
        test_both_files()

        print("\n" + "=" * 60)
        print("COMPARISON TEST COMPLETED!")
        print("Check the 'test_output' directory for CSV files:")
        print("  - submissions_CIK0000001800.csv (main file)")
        print("  - submissions_CIK0000001800_001.csv (submissions-001 file)")
        print("=" * 60)

    except Exception as e:
        print(f"Test failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
