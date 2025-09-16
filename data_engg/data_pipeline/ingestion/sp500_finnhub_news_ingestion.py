#!/usr/bin/env python3
"""
S&P 500 Finnhub News Data Ingestion Script

This script performs bulk insert of S&P 500 news data from CSV to database.
It uses the sp500_finnhub_news table created by create_tables.py

Usage:
    python sp500_finnhub_news_ingestion.py [--input-file PATH] [--batch-size SIZE] [--test-mode]
"""

import os
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
import argparse
from datetime import datetime

# Add parent directories to path for imports - system independent
current_dir = Path(__file__).parent.absolute()
data_engg_root = current_dir.parent.parent  # Go up to data_engg/
sys.path.insert(0, str(data_engg_root))

try:
    from database.config.config import Config
    from database.db_connection import engine, Session
    from database.create_tables import (
        Sp500FinnhubNews,
    )  # Import the model for the target table

    print("Database modules imported successfully")
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Data engg root: {data_engg_root}")
    print("Make sure you're running from the correct directory and modules exist.")
    sys.exit(1)


def validate_csv_structure(df: pd.DataFrame) -> bool:
    """Validate that the CSV has the expected structure for sp500_finnhub_news"""
    expected_columns = [
        "symbol",
        "id",
        "datetime",
        "headline",
        "summary",
        "source",
        "url",
        "image",
        "related",
        "category",
    ]

    # Check if all expected columns exist
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        print(f"✗ Missing columns: {missing_columns}")
        return False

    return True


def clean_and_prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare data for database insertion"""
    print("Cleaning and preparing data...")

    # Create a copy to avoid modifying the original
    df_clean = df.copy()

    # Convert datetime column
    df_clean["datetime"] = pd.to_datetime(df_clean["datetime"], errors="coerce")

    # Handle missing values
    df_clean = df_clean.fillna("")

    # Convert news_id to string
    df_clean["news_id"] = df_clean["id"].astype(str)

    # Remove the original 'id' column to avoid conflicts with auto-increment primary key
    df_clean = df_clean.drop(columns=["id"])

    # Remove rows with invalid datetime
    initial_count = len(df_clean)
    df_clean = df_clean.dropna(subset=["datetime"])
    final_count = len(df_clean)

    if initial_count != final_count:
        print(f"Removed {initial_count - final_count} rows with invalid datetime")

    print(f"✓ Data cleaning completed. Records: {len(df_clean):,}")
    return df_clean


def bulk_insert_data(
    df: pd.DataFrame, batch_size: int = 10000, test_mode: bool = False
) -> dict:
    """Perform bulk insert of data to database"""
    total_records = len(df)
    total_batches = (total_records + batch_size - 1) // batch_size
    successful_records = 0
    failed_records = 0

    print(
        f"Starting bulk insert: {total_records:,} records in {total_batches} batches..."
    )

    try:
        with Session() as session:
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, total_records)
                batch_df = df.iloc[start_idx:end_idx]

                # Show progress every 10 batches or for the last batch
                if (batch_num + 1) % 10 == 0 or (batch_num + 1) == total_batches:
                    print(
                        f"Progress: {batch_num + 1}/{total_batches} batches ({(batch_num + 1)/total_batches*100:.1f}%)"
                    )

                try:
                    # Convert DataFrame to list of dictionaries
                    records = batch_df.to_dict("records")

                    # Insert records using bulk_insert_mappings for better performance
                    session.bulk_insert_mappings(Sp500FinnhubNews, records)
                    session.commit()

                    batch_success = len(records)
                    successful_records += batch_success

                    if test_mode and batch_num >= 2:  # Limit to 3 batches in test mode
                        print("Test mode: Stopping after 3 batches")
                        break

                except Exception as e:
                    print(f"✗ Error in batch {batch_num + 1}: {e}")
                    session.rollback()
                    failed_records += len(batch_df)
                    continue

    except Exception as e:
        print(f"✗ Database connection error: {e}")
        return {"success": False, "error": str(e)}

    result = {
        "success": True,
        "total_records": total_records,
        "successful_records": successful_records,
        "failed_records": failed_records,
        "test_mode": test_mode,
    }

    print("\n" + "=" * 50)
    print("BULK INSERT SUMMARY")
    print("=" * 50)
    print(f"Total records processed: {total_records:,}")
    print(f"Successfully inserted: {successful_records:,}")
    print(f"Failed records: {failed_records:,}")
    print(f"Success rate: {(successful_records/total_records)*100:.2f}%")

    return result


def main():
    """Main function to run the ingestion process"""
    parser = argparse.ArgumentParser(
        description="S&P 500 Finnhub News Data Ingestion Script"
    )
    parser.add_argument(
        "--input-file",
        type=str,
        default="../../../data/sp500_finnhub_news_5y.csv",
        help="Path to the input CSV file",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for bulk insert (default: 10000)",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Run in test mode (limited records)",
    )

    args = parser.parse_args()

    print("S&P 500 Finnhub News Data Ingestion")
    print("=" * 50)
    print(f"Input file: {args.input_file}")
    print(f"Batch size: {args.batch_size:,}")
    print(f"Test mode: {args.test_mode}")
    print()

    # Check if input file exists
    if not os.path.exists(args.input_file):
        print(f"✗ Error: Input file '{args.input_file}' not found")
        sys.exit(1)

    try:
        # Read CSV file
        print("Reading CSV file...")
        df = pd.read_csv(args.input_file)
        print(f"✓ CSV file read successfully. Records: {len(df):,}")

        # Validate CSV structure
        if not validate_csv_structure(df):
            print("✗ CSV structure validation failed")
            sys.exit(1)

        # Clean and prepare data
        df_clean = clean_and_prepare_data(df)

        # Perform bulk insert
        result = bulk_insert_data(df_clean, args.batch_size, args.test_mode)

        if result["success"]:
            print("\n✓ Data ingestion completed successfully!")
        else:
            print(f"\n✗ Data ingestion failed: {result.get('error', 'Unknown error')}")
            sys.exit(1)

    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
