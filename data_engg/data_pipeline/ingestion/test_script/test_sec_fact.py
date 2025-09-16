#!/usr/bin/env python3
"""
Simple CSV to Database Import Script
Imports facts CSV data from test_output directory into database tables using SQLAlchemy sessions.
"""

import pandas as pd
import os
import sys
from datetime import datetime
from pathlib import Path

# Add the database directory to the path - system independent
current_dir = Path(__file__).parent.absolute()
project_root = current_dir.parent.parent.parent  # Go up to backend/
database_dir = project_root / "database"
sys.path.insert(0, str(database_dir))

try:
    from db_connection import Session
    from models.sec_facts_raw import BronzeSecFacts
    from sqlalchemy import text
except ImportError as e:
    print(f"Error importing database modules: {e}")
    print(f"Database directory: {database_dir}")
    print(
        "Make sure you're running from the correct directory and the database module exists."
    )
    sys.exit(1)


def import_csv_data():
    """Import CSV data using SQLAlchemy sessions"""

    # Path to CSV files
    csv_dir = os.path.join(os.path.dirname(__file__), "..", "test_output")
    facts_file = os.path.join(csv_dir, "facts_0000001800.csv")

    print("Starting CSV import...")

    # Import facts data
    if os.path.exists(facts_file):
        print(f"Importing facts data from: {facts_file}")
        df_facts = pd.read_csv(facts_file)

        print(f"Total records in CSV: {len(df_facts)}")

        with Session() as session:
            try:
                print(f"Loaded {len(df_facts)} rows from CSV")

                # Convert CSV rows to database records
                data_to_insert = []
                for index, row in df_facts.iterrows():
                    # Convert date strings to datetime objects
                    start_date = (
                        pd.to_datetime(row["start_date"]).to_pydatetime()
                        if pd.notna(row["start_date"])
                        else None
                    )
                    end_date = (
                        pd.to_datetime(row["end_date"]).to_pydatetime()
                        if pd.notna(row["end_date"])
                        else None
                    )
                    filed_date = (
                        pd.to_datetime(row["filed"]).to_pydatetime()
                        if pd.notna(row["filed"])
                        else None
                    )

                    data_to_insert.append(
                        {
                            "cik": row["cik"],
                            "taxonomy": row["taxonomy"],
                            "tag": row["tag"],
                            "unit": row["unit"],
                            "val": row["val"],
                            "fy": row["fy"] if pd.notna(row["fy"]) else None,
                            "fp": row["fp"] if pd.notna(row["fp"]) else None,
                            "start_date": start_date,
                            "end_date": end_date,
                            "frame": row["frame"] if pd.notna(row["frame"]) else None,
                            "form": row["form"] if pd.notna(row["form"]) else None,
                            "filed": filed_date,
                            "accn": row["accn"],
                        }
                    )

                # Create SQL insert statement
                insert_sql = text(
                    """
                    INSERT INTO bronze_sec_facts 
                    (cik, taxonomy, tag, unit, val, fy, fp, start_date, end_date, frame, form, filed, accn)
                    VALUES (:cik, :taxonomy, :tag, :unit, :val, :fy, :fp, :start_date, :end_date, :frame, :form, :filed, :accn)
                """
                )

                # Insert records in batches of 1000
                batch_size = 1000
                total_inserted = 0

                for i in range(0, len(data_to_insert), batch_size):
                    batch = data_to_insert[i : i + batch_size]
                    result = session.execute(insert_sql, batch)
                    session.commit()
                    total_inserted += len(batch)
                    print(
                        f"Processed {total_inserted}/{len(data_to_insert)} records..."
                    )

                print(f"Successfully inserted {len(data_to_insert)} facts records")

            except Exception as e:
                session.rollback()
                print(f"Error importing facts data: {e}")


if __name__ == "__main__":
    import_csv_data()
