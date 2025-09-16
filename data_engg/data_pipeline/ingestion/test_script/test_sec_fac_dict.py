#!/usr/bin/env python3
"""
Simple CSV to Database Import Script
Imports CSV data from test_output directory into database tables using SQLAlchemy sessions.
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
    from models.sec_facts_raw import BronzeSecFacts, BronzeSecFactsDict
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
    dictionary_file = os.path.join(csv_dir, "dictionary_0000001800.csv")
    facts_file = os.path.join(csv_dir, "facts_0000001800.csv")

    print("Starting CSV import...")

    # Import dictionary data
    if os.path.exists(dictionary_file):
        print(f"Importing dictionary data from: {dictionary_file}")
        df_dict = pd.read_csv(dictionary_file)

        with Session() as session:
            try:
                # Get existing records to avoid duplicates
                existing_records = session.query(BronzeSecFactsDict).all()
                existing_keys = {(r.taxonomy, r.tag) for r in existing_records}

                # Prepare new records for insertion
                records_to_insert = []
                skipped_rows = []

                for index, row in df_dict.iterrows():
                    taxonomy = row["taxonomy"]
                    tag = row["tag"]

                    # Check if record already exists
                    if (taxonomy, tag) not in existing_keys:
                        # Create dictionary record object
                        dict_record = BronzeSecFactsDict(
                            taxonomy=taxonomy,
                            tag=tag,
                            label=row["label"] if pd.notna(row["label"]) else None,
                            description=(
                                row["description"]
                                if pd.notna(row["description"])
                                else None
                            ),
                        )
                        records_to_insert.append(dict_record)
                    else:
                        # Record already exists, add to skipped list
                        skipped_rows.append(
                            {
                                "row": index + 1,  # CSV row number (1-indexed)
                                "taxonomy": taxonomy,
                                "tag": tag,
                            }
                        )

                # Insert new records into database
                if records_to_insert:
                    session.add_all(records_to_insert)
                    session.commit()
                    print(
                        f"Successfully imported {len(records_to_insert)} dictionary records"
                    )
                else:
                    print("No new records to import")

                if skipped_rows:
                    print(f"Skipped {len(skipped_rows)} existing records:")
                    for skipped in skipped_rows[:10]:  # Show first 10 skipped records
                        print(
                            f"   Row {skipped['row']}: {skipped['taxonomy']} - {skipped['tag']}"
                        )
                    if len(skipped_rows) > 10:
                        print(
                            f"   ... and {len(skipped_rows) - 10} more existing records"
                        )

            except Exception as e:
                session.rollback()
                print(f"Error importing dictionary data: {e}")

    # Skip facts data import - only importing dictionary data


if __name__ == "__main__":
    import_csv_data()
