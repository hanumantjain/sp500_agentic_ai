#!/usr/bin/env python3
"""
Database Query and Data Management Script

This script provides comprehensive database querying and data management capabilities
using db_connection.py and logic from test_json_to_db.py.

Features:
- Database Connection: Uses db_connection.py for secure database connections
- SQL Querying: Execute custom SQL queries and view results
- Data Insertion: Insert data back to database using proven logic
- Table Management: View table schemas, counts, and sample data
- Data Analysis: Analyze and summarize query results
"""

import pandas as pd
import numpy as np
from sqlalchemy import text, inspect
from sqlalchemy.orm import sessionmaker
import sys
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import argparse

# Add parent directories to path for imports - system independent
current_dir = Path(__file__).parent.absolute()
data_engg_root = current_dir.parent  # Go up to data_engg/
sys.path.insert(0, str(data_engg_root))

# Import database modules
try:
    from database.db_connection import engine, Session
    from database.config.config import Config
    from database.models.sec_facts_raw import BronzeSecFacts, BronzeSecFactsDict
    from database.models.sec_submissions_raw import BronzeSecSubmissions

    print("Database modules imported successfully")
except ImportError as e:
    print(f"Error importing database modules: {e}")
    print(f"Data engg root: {data_engg_root}")
    print("Make sure you're running from the correct directory and modules exist.")
    sys.exit(1)


def test_database_connection():
    """Test database connection and display basic info"""
    try:
        with Session() as session:
            # Test basic connection
            result = session.execute(text("SELECT 1 as test")).fetchone()
            print(f"Database connection successful: {result[0]}")

            # Get database info
            db_info = session.execute(text("SELECT DATABASE() as db_name")).fetchone()
            print(f"Connected to database: {db_info[0]}")

            return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False


def get_table_info():
    """Get information about all tables in the database"""
    try:
        with Session() as session:
            # Get all table names
            tables_query = text(
                """
                SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE()
                ORDER BY TABLE_NAME
            """
            )

            tables_df = pd.read_sql(tables_query, engine)

            print("Database Tables Overview:")
            print("=" * 60)
            print(tables_df.to_string(index=False))

            return tables_df
    except Exception as e:
        print(f"Error getting table info: {e}")
        return None


def get_record_counts():
    """Get record counts for main database tables"""
    try:
        with Session() as session:
            counts = {}

            # Count BronzeSecFacts
            try:
                facts_count = session.query(BronzeSecFacts).count()
                counts["BronzeSecFacts"] = facts_count
            except:
                counts["BronzeSecFacts"] = 0

            # Count BronzeSecFactsDict
            try:
                dict_count = session.query(BronzeSecFactsDict).count()
                counts["BronzeSecFactsDict"] = dict_count
            except:
                counts["BronzeSecFactsDict"] = 0

            # Count BronzeSecSubmissions
            try:
                submissions_count = session.query(BronzeSecSubmissions).count()
                counts["BronzeSecSubmissions"] = submissions_count
            except:
                counts["BronzeSecSubmissions"] = 0

            print("Record Counts:")
            print("=" * 30)
            for table, count in counts.items():
                print(f"{table:20}: {count:,} records")

            total_records = sum(counts.values())
            print(f"{'Total':20}: {total_records:,} records")

            return counts
    except Exception as e:
        print(f"Error getting record counts: {e}")
        return {}


def execute_sql_query(query: str, return_df: bool = True, limit: int = 1000):
    """
    Execute a custom SQL query and return results

    Args:
        query: SQL query string
        return_df: Whether to return pandas DataFrame (True) or raw results (False)
        limit: Maximum number of rows to return (safety limit)

    Returns:
        pandas DataFrame or list of results
    """
    try:
        with Session() as session:
            # Add LIMIT if not present and query is SELECT
            if (
                query.strip().upper().startswith("SELECT")
                and "LIMIT" not in query.upper()
            ):
                query = f"{query.rstrip(';')} LIMIT {limit}"

            print(f"Executing query:")
            print(f"   {query}")
            print("-" * 50)

            if return_df:
                # Use raw connection for pandas compatibility
                raw_connection = engine.raw_connection()
                result_df = pd.read_sql(query, raw_connection)
                raw_connection.close()
                print(f"Query executed successfully - {len(result_df)} rows returned")
                return result_df
            else:
                result = session.execute(text(query)).fetchall()
                print(f"Query executed successfully - {len(result)} rows returned")
                return result

    except Exception as e:
        print(f"Error executing query: {e}")
        return None


def insert_dataframe_to_table(
    df: pd.DataFrame, table_name: str, if_exists: str = "append"
):
    """
    Insert a pandas DataFrame into a database table

    Args:
        df: pandas DataFrame to insert
        table_name: Target table name
        if_exists: What to do if table exists ('append', 'replace', 'fail')

    Returns:
        Number of rows inserted
    """
    try:
        rows_inserted = df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=1000,
        )
        print(f"Successfully inserted {len(df)} rows into {table_name}")
        return len(df)
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")
        return 0


def bulk_insert_records(records: List[Dict], model_class, batch_size: int = 1000):
    """
    Bulk insert records using SQLAlchemy ORM (based on test_json_to_db.py logic)

    Args:
        records: List of dictionaries representing records
        model_class: SQLAlchemy model class
        batch_size: Number of records to insert per batch

    Returns:
        Number of records inserted
    """
    try:
        with Session() as session:
            total_inserted = 0

            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]

                # Create model instances
                model_instances = [model_class(**record) for record in batch]

                # Bulk insert
                session.bulk_save_objects(model_instances)
                session.commit()

                total_inserted += len(batch)
                print(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")

            print(f"Total records inserted: {total_inserted}")
            return total_inserted

    except Exception as e:
        print(f"Error in bulk insert: {e}")
        return 0


def analyze_cik_data(cik: str):
    """
    Analyze data for a specific CIK

    Args:
        cik: CIK number to analyze
    """
    try:
        # Get facts for this CIK
        facts_query = f"""
        SELECT tag, val, end_date, filed
        FROM bronze_sec_facts 
        WHERE cik = '{cik}'
        ORDER BY filed DESC
        LIMIT 100
        """

        facts_df = execute_sql_query(facts_query)

        if facts_df is not None and len(facts_df) > 0:
            print(f"Analysis for CIK {cik}:")
            print(f"   Total records: {len(facts_df)}")
            print(
                f"   Date range: {facts_df['filed'].min()} to {facts_df['filed'].max()}"
            )
            print(f"   Unique tags: {facts_df['tag'].nunique()}")

            # Show top tags by frequency
            top_tags = facts_df["tag"].value_counts().head(10)
            print(f"\n   Top 10 tags:")
            for tag, count in top_tags.items():
                print(f"     {tag}: {count}")

            return facts_df
        else:
            print(f"No data found for CIK {cik}")
            return None

    except Exception as e:
        print(f"Error analyzing CIK {cik}: {e}")
        return None


def display_dataframe_summary(df: pd.DataFrame):
    """
    Display a summary of the DataFrame

    Args:
        df: pandas DataFrame to summarize
    """
    try:
        print(f"\nDataFrame Summary:")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"Data types:")
        for col, dtype in df.dtypes.items():
            print(f"  {col}: {dtype}")

        print(f"\nFirst 5 rows:")
        print(df.head().to_string(index=False))

        if len(df) > 5:
            print(f"\nLast 5 rows:")
            print(df.tail().to_string(index=False))

    except Exception as e:
        print(f"Error displaying DataFrame summary: {e}")


def run_sample_queries():
    """Run sample queries to demonstrate functionality"""
    print("Sample Queries for BronzeSecFacts Table")
    print("=" * 50)

    # Query 1: Top CIKs by record count
    query1 = """
    SELECT cik, COUNT(*) as record_count
    FROM bronze_sec_facts 
    GROUP BY cik 
    ORDER BY record_count DESC 
    LIMIT 10
    """
    df1 = execute_sql_query(query1)
    if df1 is not None:
        print("\nTop 10 CIKs by record count:")
        print(df1.to_string(index=False))

    print("\n" + "=" * 50)

    # Query 2: Sample records
    query2 = """
    SELECT cik, tag, val, end_date, filed
    FROM bronze_sec_facts 
    WHERE val IS NOT NULL 
    ORDER BY filed DESC 
    LIMIT 5
    """
    df2 = execute_sql_query(query2)
    if df2 is not None:
        print("\nSample recent records:")
        print(df2.to_string(index=False))


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description="Database Query and Data Management Tool"
    )
    parser.add_argument(
        "--test-connection", action="store_true", help="Test database connection"
    )
    parser.add_argument(
        "--table-info", action="store_true", help="Show table information"
    )
    parser.add_argument(
        "--record-counts", action="store_true", help="Show record counts"
    )
    parser.add_argument("--query", type=str, help="Execute custom SQL query")
    parser.add_argument("--analyze-cik", type=str, help="Analyze data for specific CIK")
    parser.add_argument(
        "--sample-queries", action="store_true", help="Run sample queries"
    )
    parser.add_argument(
        "--limit", type=int, default=1000, help="Limit for query results"
    )

    args = parser.parse_args()

    # If no arguments provided, run basic checks
    if not any(vars(args).values()):
        print("Database Query and Data Management Tool")
        print("=" * 50)

        # Test connection
        if test_database_connection():
            print("\n" + "=" * 50)
            get_record_counts()
            print("\n" + "=" * 50)
            run_sample_queries()
    else:
        # Execute specific commands
        if args.test_connection:
            test_database_connection()

        if args.table_info:
            get_table_info()

        if args.record_counts:
            get_record_counts()

        if args.query:
            result = execute_sql_query(args.query, limit=args.limit)
            if result is not None:
                print("\nQuery Results:")
                print(
                    result.to_string(index=False)
                    if hasattr(result, "to_string")
                    else result
                )

        if args.analyze_cik:
            analyze_cik_data(args.analyze_cik)

        if args.sample_queries:
            run_sample_queries()


if __name__ == "__main__":
    main()
