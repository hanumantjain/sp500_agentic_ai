"""
Test script for JSON Extractor
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
    from data_pipeline.ingestion.json_extractor import SECFactsExtractor
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Project root: {project_root}")
    print("Make sure you're running from the correct directory and modules exist.")
    sys.exit(1)


class JSONToCSVConverter:
    """Converts JSON data to CSV format for verification"""

    def __init__(self, output_dir: str = "test_output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def convert_facts_to_csv(self, normalized_records: List[tuple], cik: str) -> str:
        """
        Convert normalized facts data to CSV format

        Args:
            normalized_records: List of (record_type, record_data) tuples
            cik: Company CIK identifier

        Returns:
            Path to the created CSV file
        """
        fact_records = []
        dict_records = []

        # Separate records by type
        for record_type, record_data in normalized_records:
            if record_type == "fact":
                fact_records.append(record_data)
            elif record_type == "dict":
                dict_records.append(record_data)

        # Create CSV files for each record type
        csv_files = []

        if fact_records:
            facts_df = pd.DataFrame(fact_records)
            facts_csv_path = self.output_dir / f"facts_{cik}.csv"
            facts_df.to_csv(facts_csv_path, index=False)
            csv_files.append(str(facts_csv_path))

        if dict_records:
            dict_df = pd.DataFrame(dict_records)
            dict_csv_path = self.output_dir / f"dictionary_{cik}.csv"
            dict_df.to_csv(dict_csv_path, index=False)
            csv_files.append(str(dict_csv_path))

        return csv_files


def test_single_file_processing():
    """Test the process_single_file method with a sample JSON file"""

    # Configuration
    json_directory = (
        "/Users/ssp/Documents/MS_CS/Projects_git/sp500_agentic_ai/data/company_facts"
    )
    db_config = Config()

    # Initialize extractor and CSV converter
    extractor = SECFactsExtractor(json_directory, db_config)
    csv_converter = JSONToCSVConverter()

    # Use specific test file
    test_file = Path(
        "/Users/ssp/Documents/MS_CS/Projects_git/sp500_agentic_ai/data/company_facts/CIK0000001800.json"
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
        normalized_records = extractor.normalize_facts_data(json_data, cik)

        # Convert normalized data to CSV files
        csv_files = csv_converter.convert_facts_to_csv(normalized_records, cik)

        # Count records by type
        fact_count = sum(1 for r_type, _ in normalized_records if r_type == "fact")
        dict_count = sum(1 for r_type, _ in normalized_records if r_type == "dict")

        print(f"Successfully processed {test_file.name}")
        print(f"Created CSV files: {csv_files}")
        print(
            f"Records processed: {len(normalized_records)} total, {dict_count} dictionaries (facts skipped)"
        )

        # Report database operation status
        if result["status"] == "success":
            print("Dictionary data successfully saved to database")
        else:
            print(f"Database save failed: {result.get('error', 'Unknown error')}")
            print("CSV files created for verification despite database error")

    except Exception as e:
        print(f"Error during testing: {str(e)}")
        raise


def main():
    """Main test function"""
    print("Starting JSON Extractor Test")

    try:
        # Test single file processing
        print("=" * 50)
        print("Testing single file processing")
        print("=" * 50)
        test_single_file_processing()

        print("=" * 50)
        print("Test completed successfully!")
        print("Check the 'test_output' directory for CSV files")
        print("=" * 50)

    except Exception as e:
        print(f"Test failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
