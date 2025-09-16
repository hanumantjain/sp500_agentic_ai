import sys
import os
from pathlib import Path

# Add parent directories to path for imports - system independent
current_dir = Path(__file__).parent.absolute()
data_engg_root = current_dir.parent.parent  # Go up to data_engg/
sys.path.insert(0, str(data_engg_root))

from database.db_connection import engine
from sqlalchemy import inspect

print("Checking database tables...")
inspector = inspect(engine)
tables = inspector.get_table_names()

print("Tables in database:")
for table in sorted(tables):
    print(f"  - {table}")

print(f"\nTotal tables: {len(tables)}")

# Check if our required tables exist
required_tables = ["bronze_sec_facts", "bronze_sec_facts_dict"]
missing_tables = [t for t in required_tables if t not in tables]

if missing_tables:
    print(f"\nMissing required tables: {missing_tables}")
    print("Need to run migrations!")
else:
    print("\nAll required tables exist!")
