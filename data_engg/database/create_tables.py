#!/usr/bin/env python3
"""
Database Table Creation Script

This script creates database tables without using Alembic migrations.
It uses SQLAlchemy to create tables directly from model definitions.

Usage:
    python create_tables.py
"""

import sys
from pathlib import Path
from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    Numeric,
    Text,
    DateTime,
    BigInteger,
    create_engine,
)
from sqlalchemy.orm import declarative_base
from datetime import datetime

# Add database directory to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

# Import database connection
try:
    from db_connection import engine, Session
    from config.config import Config

    print("Database modules imported successfully")
except ImportError as e:
    print(f"Error importing database modules: {e}")
    sys.exit(1)

Base = declarative_base()


class Sp500StockData(Base):
    """S&P 500 Stock Data Table"""

    __tablename__ = "sp500_stooq_ohcl"

    ticker = Column(String(10), primary_key=True, nullable=False)
    date = Column(Date, primary_key=True, nullable=False)
    open = Column(Numeric(precision=15, scale=4), nullable=True)
    high = Column(Numeric(precision=15, scale=4), nullable=True)
    low = Column(Numeric(precision=15, scale=4), nullable=True)
    close = Column(Numeric(precision=15, scale=4), nullable=True)
    volume = Column(BigInteger, nullable=True)

    __table_args__ = {"extend_existing": True}

    def __repr__(self):
        return f"<Sp500StockData(ticker={self.ticker}, date={self.date}, close={self.close})>"


class Sp500WikiList(Base):
    """S&P 500 Wiki List Table"""

    __tablename__ = "sp500_wik_list"

    symbol = Column(String(10), primary_key=True, nullable=False)
    date_added = Column(Date, primary_key=True, nullable=False)
    security = Column(String(255), nullable=True)
    gics_sector = Column(String(255), nullable=True)
    gics_sub_ind = Column(String(255), nullable=True)
    headquarters_loc = Column(String(255), nullable=True)
    cik = Column(String(20), nullable=True)
    founded = Column(String(50), nullable=True)

    __table_args__ = {"extend_existing": True}

    def __repr__(self):
        return f"<Sp500WikiList(symbol={self.symbol}, date_added={self.date_added}, security={self.security})>"


class Sp500ComponentChanges(Base):
    """S&P 500 Component Changes Table"""

    __tablename__ = "selected_changes_sp500"

    id = Column(Integer, primary_key=True, autoincrement=True)
    effective_date = Column(Date, nullable=False)
    added_ticker = Column(String(10), nullable=True)
    added_security = Column(String(255), nullable=True)
    removed_ticker = Column(String(10), nullable=True)
    removed_security = Column(String(255), nullable=True)
    reason = Column(Text, nullable=True)

    __table_args__ = {"extend_existing": True}

    def __repr__(self):
        return f"<Sp500ComponentChanges(effective_date={self.effective_date}, added_ticker={self.added_ticker}, removed_ticker={self.removed_ticker})>"


def create_specific_table(table_name: str):
    """Create a specific table by name"""
    try:
        print(f"Creating table: {table_name}")
        print("=" * 50)

        # Get the table class by name
        table_class = None
        for name, obj in globals().items():
            if (
                isinstance(obj, type)
                and issubclass(obj, Base)
                and hasattr(obj, "__tablename__")
                and obj.__tablename__ == table_name
            ):
                table_class = obj
                break

        if not table_class:
            print(f"✗ Table '{table_name}' not found in defined tables")
            available_tables = [
                cls.__tablename__
                for cls in Base.registry._class_registry.values()
                if hasattr(cls, "__tablename__")
            ]
            print(f"Available tables: {', '.join(available_tables)}")
            return False

        # Create the specific table
        table_class.__table__.create(engine, checkfirst=True)
        print(f"✓ Table '{table_name}' created successfully")
        return True

    except Exception as e:
        print(f"✗ Error creating table '{table_name}': {e}")
        return False


def drop_specific_table(table_name: str):
    """Drop a specific table by name"""
    try:
        print(f"Dropping table: {table_name}")
        print("=" * 50)

        # Get the table class by name
        table_class = None
        for name, obj in globals().items():
            if (
                isinstance(obj, type)
                and issubclass(obj, Base)
                and hasattr(obj, "__tablename__")
                and obj.__tablename__ == table_name
            ):
                table_class = obj
                break

        if not table_class:
            print(f"✗ Table '{table_name}' not found in defined tables")
            available_tables = [
                cls.__tablename__
                for cls in Base.registry._class_registry.values()
                if hasattr(cls, "__tablename__")
            ]
            print(f"Available tables: {', '.join(available_tables)}")
            return False

        # Drop the specific table
        table_class.__table__.drop(engine, checkfirst=True)
        print(f"✓ Table '{table_name}' dropped successfully")
        return True

    except Exception as e:
        print(f"✗ Error dropping table '{table_name}': {e}")
        return False


def delete_all_data_from_table(table_name: str):
    """Delete all data from a specific table by name"""
    try:
        print(f"Deleting all data from table: {table_name}")
        print("=" * 50)

        # Get the table class by name
        table_class = None
        for name, obj in globals().items():
            if (
                isinstance(obj, type)
                and issubclass(obj, Base)
                and hasattr(obj, "__tablename__")
                and obj.__tablename__ == table_name
            ):
                table_class = obj
                break

        if not table_class:
            print(f"✗ Table '{table_name}' not found in defined tables")
            available_tables = [
                cls.__tablename__
                for cls in Base.registry._class_registry.values()
                if hasattr(cls, "__tablename__")
            ]
            print(f"Available tables: {', '.join(available_tables)}")
            return False

        # Delete all data from the table
        with Session() as session:
            # Get count before deletion
            count_query = session.query(table_class).count()
            print(f"Records to be deleted: {count_query:,}")

            if count_query == 0:
                print("✓ Table is already empty")
                return True

            # Delete all records
            session.query(table_class).delete()
            session.commit()

            print(f"✓ Successfully deleted {count_query:,} records from '{table_name}'")
            return True

    except Exception as e:
        print(f"✗ Error deleting data from table '{table_name}': {e}")
        return False


def list_available_tables():
    print("Available Tables:")
    print("=" * 50)

    available_tables = []
    for name, obj in globals().items():
        if (
            isinstance(obj, type)
            and issubclass(obj, Base)
            and hasattr(obj, "__tablename__")
        ):
            table_name = obj.__tablename__
            doc = obj.__doc__ or "No description"
            available_tables.append((table_name, doc))

    for table_name, description in sorted(available_tables):
        print(f"  - {table_name:<25} : {description}")

    return [table[0] for table in available_tables]


def create_all_tables():
    try:
        print("Creating database tables...")
        print("=" * 50)

        # Create all tables
        Base.metadata.create_all(engine)

        print("Successfully created the following tables:")
        for table_name in Base.metadata.tables.keys():
            print(f"  - {table_name}")

        print("\nTable creation completed successfully!")
        return True

    except Exception as e:
        print(f"Error creating tables: {e}")
        return False


def drop_all_tables():
    """Drop all tables defined in this script (use with caution!)"""
    try:
        print("Dropping database tables...")
        print("=" * 50)

        # Drop all tables
        Base.metadata.drop_all(engine)

        print("Successfully dropped all tables!")
        return True

    except Exception as e:
        print(f"Error dropping tables: {e}")
        return False


def show_table_info():
    """Show information about existing tables"""
    try:
        with Session() as session:
            # Get table information
            tables_query = """
                SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE()
                ORDER BY TABLE_NAME
            """

            result = session.execute(tables_query)
            tables = result.fetchall()

            print("Existing Tables:")
            print("=" * 60)
            print(
                f"{'Table Name':<25} {'Rows':<15} {'Data Size':<15} {'Index Size':<15}"
            )
            print("-" * 60)

            for table in tables:
                table_name, rows, data_size, index_size = table
                rows_str = f"{rows:,}" if rows else "N/A"
                data_str = f"{data_size:,}" if data_size else "N/A"
                index_str = f"{index_size:,}" if index_size else "N/A"
                print(f"{table_name:<25} {rows_str:<15} {data_str:<15} {index_str:<15}")

    except Exception as e:
        print(f"Error getting table info: {e}")


def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description="Database Table Creation Tool")
    parser.add_argument("--create", action="store_true", help="Create all tables")
    parser.add_argument(
        "--drop", action="store_true", help="Drop all tables (DANGEROUS!)"
    )
    parser.add_argument(
        "--show-tables", action="store_true", help="Show existing tables"
    )
    parser.add_argument(
        "--recreate", action="store_true", help="Drop and recreate all tables"
    )
    parser.add_argument(
        "--create-table", type=str, help="Create a specific table by name"
    )
    parser.add_argument("--drop-table", type=str, help="Drop a specific table by name")
    parser.add_argument(
        "--delete-data", type=str, help="Delete all data from a specific table by name"
    )
    parser.add_argument(
        "--list-available", action="store_true", help="List available table definitions"
    )

    args = parser.parse_args()

    # Test database connection first
    try:
        with Session() as session:
            session.execute("SELECT 1")
        print("Database connection successful")
    except Exception as e:
        print(f"Database connection failed: {e}")
        return

    # Execute commands
    if args.show_tables:
        show_table_info()

    if args.drop:
        confirm = input(
            "Are you sure you want to drop all tables? This will DELETE ALL DATA! (yes/no): "
        )
        if confirm.lower() == "yes":
            drop_all_tables()
        else:
            print("Operation cancelled")

    if args.recreate:
        confirm = input(
            "Are you sure you want to recreate all tables? This will DELETE ALL DATA! (yes/no): "
        )
        if confirm.lower() == "yes":
            drop_all_tables()
            create_all_tables()
        else:
            print("Operation cancelled")

    if args.create:
        create_all_tables()

    if args.create_table:
        create_specific_table(args.create_table)

    if args.drop_table:
        confirm = input(
            f"Are you sure you want to drop table '{args.drop_table}'? This will DELETE ALL DATA in this table! (yes/no): "
        )
        if confirm.lower() == "yes":
            drop_specific_table(args.drop_table)
        else:
            print("Operation cancelled")

    if args.delete_data:
        confirm = input(
            f"Are you sure you want to delete ALL DATA from table '{args.delete_data}'? This action cannot be undone! (yes/no): "
        )
        if confirm.lower() == "yes":
            delete_all_data_from_table(args.delete_data)
        else:
            print("Operation cancelled")

    if args.list_available:
        list_available_tables()

    # If no arguments provided, show help
    if not any(vars(args).values()):
        print("Database Table Creation Tool")
        print("=" * 50)
        print("Available commands:")
        print("  --create              Create all tables")
        print("  --drop                Drop all tables (DANGEROUS!)")
        print("  --recreate            Drop and recreate all tables (DANGEROUS!)")
        print("  --show-tables         Show existing tables")
        print("  --create-table NAME   Create a specific table by name")
        print("  --drop-table NAME     Drop a specific table by name")
        print(
            "  --delete-data NAME    Delete all data from a specific table (DANGEROUS!)"
        )
        print("  --list-available      List available table definitions")
        print("\nExample usage:")
        print("  python create_tables.py --create")
        print("  python create_tables.py --create-table sp500_stooq_ohcl")
        print("  python create_tables.py --drop-table sp500_wik_list")
        print("  python create_tables.py --delete-data sp500_stooq_ohcl")
        print("  python create_tables.py --list-available")


if __name__ == "__main__":
    main()
