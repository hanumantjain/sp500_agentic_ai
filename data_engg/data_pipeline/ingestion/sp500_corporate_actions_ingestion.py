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
        Sp500CorporateActions,
    )  # Import the model for the target table

    print("Database modules imported successfully")
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Project root: {data_engg_root}")
    print("Make sure you're running from the correct directory and modules exist.")
    sys.exit(1)


def validate_csv_structure(df: pd.DataFrame) -> bool:
    """Validate that the CSV has the expected structure for sp500_corporate_actions"""
    expected_columns = [
        "symbol",
        "event_type",
        "ex_date",
        "record_date",
        "pay_date",
        "ratio",
        "cash_amount",
    ]

    # Check if all expected columns exist
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        print(f"✗ Missing columns: {missing_columns}")
        return False

    print("✓ CSV structure validation passed")
    return True


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare the data for insertion"""
    print("Cleaning data...")

    # Create a copy to avoid modifying the original
    df_clean = df.copy()

    # Convert date columns
    date_columns = ["ex_date", "record_date", "pay_date"]
    for col in date_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors="coerce")

    # Convert cash_amount to numeric
    if "cash_amount" in df_clean.columns:
        df_clean["cash_amount"] = pd.to_numeric(
            df_clean["cash_amount"], errors="coerce"
        )

    # Clean string columns
    string_columns = ["symbol", "event_type", "ratio"]
    for col in string_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()

    # Remove rows where symbol is null or empty
    df_clean = df_clean.dropna(subset=["symbol"])
    df_clean = df_clean[df_clean["symbol"] != ""]

    print(f"✓ Data cleaned. {len(df_clean)} records remaining after cleaning")
    return df_clean


def insert_data_to_database(df: pd.DataFrame, session: Session) -> bool:
    """Insert the cleaned data into the database"""
    print("Inserting data into database...")

    try:
        # Get existing records count
        existing_count = session.query(Sp500CorporateActions).count()
        print(f"Existing records in database: {existing_count}")

        # Clear existing data first (since symbol is primary key, we'll replace all)
        if existing_count > 0:
            print("Clearing existing data...")
            session.query(Sp500CorporateActions).delete()
            session.commit()

        # Prepare data for bulk insertion
        records_to_insert = []
        for _, row in df.iterrows():
            record = Sp500CorporateActions(
                symbol=row["symbol"],
                event_type=row["event_type"],
                ex_date=row["ex_date"] if pd.notna(row["ex_date"]) else None,
                record_date=(
                    row["record_date"] if pd.notna(row["record_date"]) else None
                ),
                pay_date=row["pay_date"] if pd.notna(row["pay_date"]) else None,
                ratio=(
                    row["ratio"]
                    if pd.notna(row["ratio"]) and row["ratio"] != ""
                    else None
                ),
                cash_amount=(
                    row["cash_amount"] if pd.notna(row["cash_amount"]) else None
                ),
            )
            records_to_insert.append(record)

        # Bulk insert
        print(f"Inserting {len(records_to_insert)} records...")
        session.bulk_save_objects(records_to_insert)
        session.commit()

        # Get final count
        final_count = session.query(Sp500CorporateActions).count()
        print(f"✓ Data insertion completed successfully")
        print(f"Records inserted: {len(records_to_insert)}")
        print(f"Total records in database: {final_count}")

        return True

    except Exception as e:
        print(f"✗ Error inserting data: {e}")
        session.rollback()
        return False


def main():
    """Main function to process the corporate actions CSV file"""
    parser = argparse.ArgumentParser(
        description="Process S&P 500 Corporate Actions CSV file"
    )
    parser.add_argument(
        "--csv-file",
        type=str,
        default=str(
            Path(__file__).parent.parent.parent.parent
            / "data"
            / "sp500_corporate_actions_yfinance.csv"
        ),
        help="Path to the CSV file (relative to ingestion directory)",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate the CSV structure without inserting data",
    )

    args = parser.parse_args()

    print("S&P 500 Corporate Actions Data Ingestion")
    print("=" * 50)

    # Get the CSV file path
    csv_file_path = Path(args.csv_file)
    if not csv_file_path.is_absolute():
        csv_file_path = current_dir / csv_file_path

    print(f"CSV file path: {csv_file_path}")

    # Check if file exists
    if not csv_file_path.exists():
        print(f"✗ Error: CSV file not found at {csv_file_path}")
        return False

    try:
        # Read the CSV file
        print("Reading CSV file...")
        df = pd.read_csv(csv_file_path)
        print(f"✓ CSV file read successfully. Shape: {df.shape}")

        # Validate CSV structure
        if not validate_csv_structure(df):
            print("✗ CSV structure validation failed")
            return False

        if args.validate_only:
            print("✓ Validation completed successfully (validate-only mode)")
            return True

        # Clean the data
        df_clean = clean_data(df)

        # Connect to database and insert data
        session = Session()
        try:
            success = insert_data_to_database(df_clean, session)
            if success:
                print("✓ Corporate actions data ingestion completed successfully!")
                return True
            else:
                print("✗ Corporate actions data ingestion failed")
                return False
        finally:
            session.close()

    except Exception as e:
        print(f"✗ Error processing CSV file: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
