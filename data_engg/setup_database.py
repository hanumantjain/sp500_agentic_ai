#!/usr/bin/env python3
"""
SP500 Agentic AI - Database Setup Script
This script sets up the database tables using Alembic migrations
Supports different table groups (facts, submissions, etc.)
"""

import sys
import os
import argparse
from pathlib import Path

# Add current directory to Python path
sys.path.append(".")


def test_database_connection():
    """Test database connection and show current tables"""
    try:
        from database.db_connection import engine
        from sqlalchemy import inspect

        print("Testing database connection...")
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        print("✓ Database connection successful!")
        print(f"Current tables in database: {len(tables)}")

        if tables:
            print("Existing tables:")
            for table in sorted(tables):
                print(f"  - {table}")
        else:
            print("No tables found (empty database)")

        return True, tables

    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False, []


def show_table_info(setup_type, tables_to_setup):
    """Show information about tables to be created"""
    print(f"Setting up {setup_type} tables:")

    table_info = {
        "bronze_sec_facts": "SEC company facts data with financial metrics",
        "bronze_sec_facts_dict": "Data dictionary for SEC facts tags and labels",
        "bronze_sec_submissions": "SEC filing submissions with metadata",
    }

    for table in tables_to_setup:
        if table in table_info:
            print(f"  - {table}: {table_info[table]}")
        else:
            print(f"  - {table}")

    if "bronze_sec_submissions" in tables_to_setup:
        print("\nSubmissions table schema:")
        print(
            "  - Composite Primary Key: (cik, accession_number, filing_date, acceptance_datetime)"
        )
        print("  - 15 data columns + 2 audit columns (created_at, updated_at)")


def drop_tables(tables_to_drop):
    """Drop existing tables if they exist"""
    try:
        from database.db_connection import engine
        from sqlalchemy import inspect, text

        print(f"Checking for existing tables to drop: {tables_to_drop}")
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        existing_tables = [t for t in tables_to_drop if t in tables]

        if existing_tables:
            print(f"Dropping existing tables: {existing_tables}")
            with engine.begin() as conn:
                for table in existing_tables:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
            print("✓ Existing tables dropped successfully!")
        else:
            print("No existing tables to drop")

        return True

    except Exception as e:
        print(f"✗ Error dropping tables: {e}")
        return False


def create_tables_directly(tables_to_create):
    """Create tables directly using SQLAlchemy models"""
    try:
        from database.db_connection import engine

        print("Creating tables directly using SQLAlchemy...")

        # Import all models to ensure they're registered
        from database.models.sec_facts_raw import Base as FactsBase
        from database.models.sec_facts_raw import BronzeSecFacts, BronzeSecFactsDict

        from database.models.sec_submissions_raw import Base as SubmissionsBase
        from database.models.sec_submissions_raw import BronzeSecSubmissions

        # Create tables based on what's needed
        if any(
            table in tables_to_create
            for table in ["bronze_sec_facts", "bronze_sec_facts_dict"]
        ):
            print("Creating facts tables...")
            FactsBase.metadata.create_all(engine)

        if "bronze_sec_submissions" in tables_to_create:
            print("Creating submissions table...")
            SubmissionsBase.metadata.create_all(engine)

        print("✓ Tables created successfully!")
        return True

    except Exception as e:
        print(f"✗ Error creating tables: {e}")
        return False


def verify_tables(required_tables):
    """Verify that required tables were created"""
    try:
        from database.db_connection import engine
        from sqlalchemy import inspect

        print("Verifying tables were created...")
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        missing_tables = [t for t in required_tables if t not in tables]

        if missing_tables:
            print(f"✗ Missing required tables: {missing_tables}")
            return False
        else:
            print("✓ All required tables created successfully!")
            print("Final database tables:")
            for table in sorted(tables):
                print(f"  - {table}")
            return True

    except Exception as e:
        print(f"✗ Error verifying tables: {e}")
        return False


def setup_database(tables_to_setup, setup_type="facts"):
    """Main setup function with configurable tables"""
    print("=" * 50)
    print(f"SP500 Agentic AI - Database Setup ({setup_type.title()})")
    print("=" * 50)

    # Step 1: Show table information
    show_table_info(setup_type, tables_to_setup)

    # Step 2: Test database connection
    print("\n" + "-" * 30)
    success, current_tables = test_database_connection()
    if not success:
        print("\n✗ Cannot proceed without database connection")
        return False

    # Step 3: Show what will be done
    print("\n" + "-" * 30)
    print("This will:")
    print(f"  1. Drop existing tables (if any): {', '.join(tables_to_setup)}")
    print("  2. Create fresh tables using SQLAlchemy models")
    print("  3. Reset all data (existing data will be lost)")

    # Step 4: Ask for confirmation
    response = input(
        f"\nDo you want to proceed with {setup_type} database setup? (y/N): "
    )
    if response.lower() not in ["y", "yes"]:
        print("Database setup cancelled")
        return False

    # Step 5: Drop existing tables
    print("\n" + "-" * 30)
    if not drop_tables(tables_to_setup):
        return False

    # Step 6: Create tables directly
    print("\n" + "-" * 30)
    if not create_tables_directly(tables_to_setup):
        return False

    # Step 7: Verify tables
    print("\n" + "-" * 30)
    if not verify_tables(tables_to_setup):
        return False

    # Success!
    print("\n" + "=" * 50)
    print(f"✓ {setup_type.title()} database setup completed successfully!")
    print(f"Your database is ready for SEC {setup_type} data ingestion")
    print("\nNext steps:")
    print("1. Run your Airflow DAG to start data ingestion")
    print("2. Monitor the data pipeline logs")
    print("=" * 50)
    return True


def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Setup SEC database tables")
    parser.add_argument(
        "--type",
        choices=["facts", "submissions", "all"],
        default="facts",
        help="Type of tables to setup",
    )

    args = parser.parse_args()

    # Define table groups
    table_groups = {
        "facts": ["bronze_sec_facts", "bronze_sec_facts_dict"],
        "submissions": ["bronze_sec_submissions"],
        "all": ["bronze_sec_facts", "bronze_sec_facts_dict", "bronze_sec_submissions"],
    }

    tables_to_setup = table_groups[args.type]

    success = setup_database(tables_to_setup, args.type)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
