#!/usr/bin/env python3
"""
S&P 500 Stooq OHLC Data Ingestion Script

This script performs bulk insert of normalized S&P 500 stock data from CSV to database.
It uses the sp500_stooq_ohcl table created by create_tables.py

Usage:
    python sp500_stooq_ohcl_ingestion.py [--input-file PATH] [--batch-size SIZE] [--test-mode]
"""

import pandas as pd
import sys
import os
from pathlib import Path
from typing import Optional
import argparse
from datetime import datetime
import time

# Add parent directories to path for imports
current_dir = Path(__file__).parent.absolute()
project_root = current_dir.parent.parent  # Go up to backend/
sys.path.insert(0, str(project_root))

try:
    from database.db_connection import engine, Session
    from database.config.config import Config
    from database.create_tables import Sp500StockData

    print("Database modules imported successfully")
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Project root: {project_root}")
    sys.exit(1)


def validate_csv_structure(df: pd.DataFrame) -> bool:
    """Validate that CSV has the expected structure"""
    expected_columns = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]

    if not all(col in df.columns for col in expected_columns):
        print(f"Error: CSV missing required columns. Expected: {expected_columns}")
        print(f"Found: {list(df.columns)}")
        return False

    print(f"CSV structure validation passed. Shape: {df.shape}")
    return True


def clean_and_prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare data for database insertion"""
    print("Cleaning and preparing data...")

    # Create a copy to avoid modifying original
    df_clean = df.copy()

    # Rename columns to match database schema
    column_mapping = {
        "Ticker": "ticker",
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    }

    df_clean = df_clean.rename(columns=column_mapping)

    # Convert date column
    df_clean["date"] = pd.to_datetime(df_clean["date"]).dt.date

    # Convert numeric columns, handling any non-numeric values
    numeric_columns = ["open", "high", "low", "close", "volume"]
    for col in numeric_columns:
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

    # Remove rows with any NaN values
    initial_count = len(df_clean)
    df_clean = df_clean.dropna()
    final_count = len(df_clean)

    if initial_count != final_count:
        print(f"Removed {initial_count - final_count} rows with missing values")

    # Remove duplicates based on ticker and date (composite primary key)
    initial_count = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["ticker", "date"], keep="last")
    final_count = len(df_clean)

    if initial_count != final_count:
        print(f"Removed {initial_count - final_count} duplicate rows")

    print(f"Final clean data shape: {df_clean.shape}")
    return df_clean


def bulk_insert_data(
    df: pd.DataFrame, batch_size: int = 50000, test_mode: bool = False
) -> dict:
    """
    Perform bulk insert of data to database

    Args:
        df: Cleaned pandas DataFrame
        batch_size: Number of records to insert per batch
        test_mode: If True, only process first 1000 records

    Returns:
        Dictionary with insertion results
    """
    if test_mode:
        df = df.head(1000)
        print(f"TEST MODE: Processing only first 1000 records")

    total_records = len(df)
    print(f"Starting bulk insert of {total_records:,} records...")
    print(f"Batch size: {batch_size:,}")

    start_time = time.time()
    total_inserted = 0
    total_errors = 0

    try:
        with Session() as session:
            # Process data in batches
            for i in range(0, total_records, batch_size):
                batch_end = min(i + batch_size, total_records)
                batch_df = df.iloc[i:batch_end]

                try:
                    # Convert DataFrame to list of dictionaries
                    batch_records = batch_df.to_dict("records")

                    # Create model instances
                    model_instances = []
                    for record in batch_records:
                        model_instances.append(Sp500StockData(**record))

                    # Bulk insert using SQLAlchemy
                    session.bulk_save_objects(model_instances)
                    session.commit()

                    total_inserted += len(batch_records)

                    # Progress update
                    progress = (batch_end / total_records) * 100
                    print(
                        f"Progress: {progress:.1f}% - Inserted {total_inserted:,}/{total_records:,} records"
                    )

                except Exception as e:
                    print(f"Error in batch {i//batch_size + 1}: {e}")
                    session.rollback()
                    total_errors += len(batch_records)
                    continue

        end_time = time.time()
        duration = end_time - start_time

        result = {
            "status": "success",
            "total_records": total_records,
            "inserted": total_inserted,
            "errors": total_errors,
            "duration_seconds": duration,
            "records_per_second": total_inserted / duration if duration > 0 else 0,
        }

        print(f"\nBulk insert completed!")
        print(f"Total records: {total_records:,}")
        print(f"Successfully inserted: {total_inserted:,}")
        print(f"Errors: {total_errors:,}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Rate: {result['records_per_second']:.0f} records/second")

        return result

    except Exception as e:
        print(f"Fatal error during bulk insert: {e}")
        return {
            "status": "failed",
            "error": str(e),
            "inserted": total_inserted,
            "errors": total_records - total_inserted,
        }


def verify_insertion(expected_count: int) -> bool:
    """Verify that data was inserted correctly"""
    try:
        with Session() as session:
            actual_count = session.query(Sp500StockData).count()

            print(f"\nVerification:")
            print(f"Expected records: {expected_count:,}")
            print(f"Actual records in DB: {actual_count:,}")

            if actual_count == expected_count:
                print("✓ Data insertion verified successfully!")
                return True
            else:
                print(f"⚠ Warning: Record count mismatch!")
                return False

    except Exception as e:
        print(f"Error during verification: {e}")
        return False


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="S&P 500 Stooq OHLC Data Ingestion")
    parser.add_argument(
        "--input-file",
        type=str,
        default="/Users/ssp/Documents/MS_CS/Projects_git/sp500_agentic_ai/data/normalised_data/normalized_sp500_data.csv",
        help="Path to input CSV file",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Batch size for bulk insert (default: 10000)",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Test mode: process only first 1000 records",
    )
    parser.add_argument(
        "--verify", action="store_true", help="Verify insertion after completion"
    )

    args = parser.parse_args()

    print("S&P 500 Stooq OHLC Data Ingestion")
    print("=" * 50)

    # Check if input file exists
    if not os.path.exists(args.input_file):
        print(f"Error: Input file not found: {args.input_file}")
        sys.exit(1)

    print(f"Input file: {args.input_file}")
    print(f"Batch size: {args.batch_size:,}")
    print(f"Test mode: {args.test_mode}")

    try:
        # Test database connection
        with Session() as session:
            session.execute("SELECT 1")
        print("✓ Database connection successful")
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        sys.exit(1)

    try:
        # Read CSV file
        print(f"\nReading CSV file...")
        df = pd.read_csv(args.input_file)
        print(f"✓ CSV file read successfully. Shape: {df.shape}")

        # Validate structure
        if not validate_csv_structure(df):
            sys.exit(1)

        # Clean and prepare data
        df_clean = clean_and_prepare_data(df)

        # Perform bulk insert
        result = bulk_insert_data(df_clean, args.batch_size, args.test_mode)

        # Verify insertion if requested
        if args.verify and result["status"] == "success":
            verify_insertion(result["inserted"])

        if result["status"] == "success":
            print(f"\n✓ Data ingestion completed successfully!")
        else:
            print(f"\n✗ Data ingestion failed!")
            sys.exit(1)

    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
