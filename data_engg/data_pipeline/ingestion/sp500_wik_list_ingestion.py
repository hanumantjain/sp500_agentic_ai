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
        Sp500WikiList,
    )  # Import the model for the target table

    print("Database modules imported successfully")
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Project root: {project_root}")
    print("Make sure you're running from the correct directory and modules exist.")
    sys.exit(1)


def validate_csv_structure(df: pd.DataFrame) -> bool:
    """Validate that the CSV has the expected structure for sp500_wik_list"""
    expected_columns = [
        "Symbol",
        "Security",
        "GICSÊSector",
        "GICS Sub-Industry",
        "Headquarters Location",
        "Date added",
        "CIK",
        "Founded",
    ]

    print("CSV structure validation:")
    print(f"Expected columns: {expected_columns}")
    print(f"Actual columns: {list(df.columns)}")

    # Check if all expected columns exist
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        print(f"✗ Missing columns: {missing_columns}")
        return False

    print("✓ CSV structure validation passed")
    return True


def clean_and_prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare the data for insertion"""
    print("Cleaning and preparing data...")

    # Create a copy to work with
    df_clean = df.copy()

    # Rename columns to match database schema
    column_mapping = {
        "Symbol": "symbol",
        "Security": "security",
        "GICSÊSector": "gics_sector",
        "GICS Sub-Industry": "gics_sub_ind",
        "Headquarters Location": "headquarters_loc",
        "Date added": "date_added",
        "CIK": "cik",
        "Founded": "founded",
    }

    df_clean = df_clean.rename(columns=column_mapping)

    # Convert date_added to datetime
    df_clean["date_added"] = pd.to_datetime(df_clean["date_added"], errors="coerce")

    # Clean CIK - remove any non-numeric characters, pad with zeros, and add CIK prefix
    df_clean["cik"] = df_clean["cik"].astype(str).str.replace(r"[^0-9]", "", regex=True)
    df_clean["cik"] = "CIK" + df_clean["cik"].str.zfill(10)

    # Clean founded year - extract year if it contains additional text
    df_clean["founded"] = df_clean["founded"].astype(str).str.extract(r"(\d{4})")[0]

    # Remove rows with missing primary key values
    initial_count = len(df_clean)
    df_clean = df_clean.dropna(subset=["symbol", "date_added"])
    final_count = len(df_clean)

    if initial_count != final_count:
        print(
            f"Removed {initial_count - final_count} rows with missing primary key values"
        )

    print(f"Final clean data shape: {df_clean.shape}")
    return df_clean


def bulk_insert_data(
    df: pd.DataFrame, batch_size: int = 1000, test_mode: bool = False
) -> dict:
    """
    Bulk insert data into the database using UPSERT (INSERT ... ON DUPLICATE KEY UPDATE)

    Args:
        df: DataFrame with cleaned data
        batch_size: Number of records to process in each batch
        test_mode: If True, only process first batch for testing

    Returns:
        dict: Results summary
    """
    print(f"Starting bulk insert of {len(df):,} records...")
    print(f"Batch size: {batch_size:,}")

    if test_mode:
        print("TEST MODE: Processing only first batch")
        df = df.head(batch_size)

    total_records = len(df)
    successful_inserts = 0
    errors = 0
    start_time = datetime.now()

    with Session() as session:
        try:
            # Process data in batches
            for i in range(0, total_records, batch_size):
                batch_df = df.iloc[i : i + batch_size]
                batch_num = (i // batch_size) + 1

                print(f"Processing batch {batch_num} ({len(batch_df):,} records)...")

                try:
                    # Convert DataFrame to list of dictionaries
                    batch_records = batch_df.to_dict("records")

                    # Create model instances
                    model_instances = []
                    for record in batch_records:
                        model_instances.append(Sp500WikiList(**record))

                    # Bulk insert using SQLAlchemy
                    session.bulk_save_objects(model_instances)
                    session.commit()

                    successful_inserts += len(batch_records)
                    print(f"✓ Batch {batch_num} inserted successfully")

                except Exception as e:
                    print(f"✗ Error in batch {batch_num}: {e}")
                    session.rollback()
                    errors += len(batch_records)
                    continue

                # Progress update
                progress = (i + len(batch_df)) / total_records * 100
                print(
                    f"Progress: {progress:.1f}% - Inserted {successful_inserts:,}/{total_records:,} records"
                )

                if test_mode:
                    break

        except Exception as e:
            print(f"✗ Fatal error during bulk insert: {e}")
            session.rollback()
            return {
                "success": False,
                "total_records": total_records,
                "successful_inserts": successful_inserts,
                "errors": errors,
                "duration": (datetime.now() - start_time).total_seconds(),
            }

    duration = (datetime.now() - start_time).total_seconds()
    rate = successful_inserts / duration if duration > 0 else 0

    print(f"\nBulk insert completed!")
    print(f"Total records: {total_records:,}")
    print(f"Successfully inserted: {successful_inserts:,}")
    print(f"Errors: {errors:,}")
    print(f"Duration: {duration:.2f} seconds")
    print(f"Rate: {rate:.0f} records/second")

    return {
        "success": True,
        "total_records": total_records,
        "successful_inserts": successful_inserts,
        "errors": errors,
        "duration": duration,
        "rate": rate,
    }


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="S&P 500 Wiki List Data Ingestion")
    parser.add_argument(
        "--input-file",
        type=str,
        default="../../../data/S_and_P_500_component_stocks.csv",
        help="Path to input CSV file",
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for bulk insert"
    )
    parser.add_argument(
        "--test-mode", action="store_true", help="Test mode (process only first batch)"
    )

    args = parser.parse_args()

    print("S&P 500 Wiki List Data Ingestion")
    print("=" * 50)
    print(f"Input file: {args.input_file}")
    print(f"Batch size: {args.batch_size:,}")
    print(f"Test mode: {args.test_mode}")

    # Test database connection
    try:
        with Session() as session:
            session.execute("SELECT 1")
        print("✓ Database connection successful")
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return

    # Check if input file exists
    if not os.path.exists(args.input_file):
        print(f"✗ Input file not found: {args.input_file}")
        return

    # Read CSV file
    print("\nReading CSV file...")
    try:
        df = pd.read_csv(args.input_file, encoding="latin-1")
        print(f"✓ CSV file read successfully. Shape: {df.shape}")
    except Exception as e:
        print(f"✗ Error reading CSV file: {e}")
        return

    # Validate CSV structure
    if not validate_csv_structure(df):
        print("✗ CSV structure validation failed")
        return

    # Clean and prepare data
    df_clean = clean_and_prepare_data(df)

    # Perform bulk insert
    result = bulk_insert_data(df_clean, args.batch_size, args.test_mode)

    if result["success"]:
        print("\n✓ Data ingestion completed successfully!")
    else:
        print("\n✗ Data ingestion failed!")
        return


if __name__ == "__main__":
    main()
