import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import engine
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
