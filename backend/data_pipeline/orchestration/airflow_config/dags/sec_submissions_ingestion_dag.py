"""
SEC Submissions JSON Ingestion DAG
Replicates the test_json_submissions_to_db.py functionality for production use
Processes all submissions JSON files and loads to bronze_sec_submissions table
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import sys
from pathlib import Path

# Add backend directory to Python path for imports
backend_root = Path(__file__).parent.parent.parent.parent.parent
if str(backend_root) not in sys.path:
    sys.path.insert(0, str(backend_root))

# Default arguments for the DAG
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "sec_submissions_ingestion",
    default_args=default_args,
    description="Process all SEC Submissions JSON files and load to database",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["sec", "submissions", "json", "ingestion", "batch"],
)


def display_submissions_database_counts(**context):
    """Display submissions database counts and compare with processing metrics"""
    try:
        from database.db_connection import Session
        from database.models.sec_submissions_raw import BronzeSecSubmissions

        # Get processing metrics from previous task
        processing_metrics = context["task_instance"].xcom_pull(
            task_ids="process_all_submissions_files"
        )

        with Session() as session:
            # Count records in bronze_sec_submissions table
            db_submissions_count = session.query(BronzeSecSubmissions).count()

            print("\n=== SUBMISSIONS DATABASE vs PROCESSING COMPARISON ===")
            print(
                f"Files Processed: {processing_metrics.get('files_processed', 'N/A')}"
            )
            print(
                f"Files Successful: {processing_metrics.get('files_successful', 'N/A')}"
            )
            print(f"Files Failed: {processing_metrics.get('files_failed', 'N/A')}")
            print()

            print("SUBMISSIONS TABLE:")
            print(
                f"   Processing Inserted: {processing_metrics.get('inserted_submissions', 0):,}"
            )
            print(f"   Database Count:      {db_submissions_count:,}")
            submissions_diff = db_submissions_count - processing_metrics.get(
                "inserted_submissions", 0
            )
            print(f"   Difference:          {submissions_diff:,}")
            if processing_metrics.get("inserted_submissions", 0) > 0:
                submissions_match = "MATCH" if submissions_diff == 0 else "MISMATCH"
                print(f"   Status:              {submissions_match}")

            # Show sample CIK values and forms if any records exist
            if db_submissions_count > 0:
                sample_ciks = (
                    session.query(BronzeSecSubmissions.cik).distinct().limit(5).all()
                )
                cik_list = [cik[0] for cik in sample_ciks]
                print(f"\nSample CIK values in database: {cik_list}")

                # Show sample forms
                sample_forms = (
                    session.query(BronzeSecSubmissions.form).distinct().limit(10).all()
                )
                form_list = [form[0] for form in sample_forms if form[0]]
                print(f"Sample forms: {form_list}")

            return db_submissions_count

    except Exception as e:
        print(f"Error querying submissions database: {e}")
        import traceback

        traceback.print_exc()
        return 0


def scan_submissions_files():
    """Scan and return list of all JSON files in submissions_facts directory"""
    try:
        # Path to the JSON files directory (relative to project root)
        project_root = backend_root.parent
        json_directory = project_root / "data" / "submissions_facts"
        json_dir = Path(json_directory)

        if not json_dir.exists():
            print(f"Submissions JSON directory not found: {json_directory}")
            return []

        # Find all JSON files
        json_files = list(json_dir.glob("*.json"))

        if not json_files:
            print(f"No JSON files found in {json_directory}")
            return []

        # Sort files to process main files first, then submissions-001, then submissions-002
        def sort_key(file_path):
            name = file_path.name
            if "-submissions-002" in name:
                return (2, name)  # Process last
            elif "-submissions-001" in name:
                return (1, name)  # Process second
            else:
                return (0, name)  # Process first (main files)

        json_files.sort(key=sort_key)

        print(f"Found {len(json_files)} submissions JSON files to process")
        print(f"First 5 files: {[f.name for f in json_files[:5]]}")
        print(f"Last 5 files: {[f.name for f in json_files[-5:]]}")

        # Count by type
        main_files = [f for f in json_files if "-submissions-" not in f.name]
        sub001_files = [f for f in json_files if "-submissions-001" in f.name]
        sub002_files = [f for f in json_files if "-submissions-002" in f.name]

        print(f"File breakdown:")
        print(f"  Main files: {len(main_files)}")
        print(f"  Submissions-001 files: {len(sub001_files)}")
        print(f"  Submissions-002 files: {len(sub002_files)}")

        # Return file paths as strings for XCom
        file_paths = [str(f) for f in json_files]
        return file_paths

    except Exception as e:
        print(f"Error scanning submissions JSON files: {e}")
        import traceback

        traceback.print_exc()
        return []


def process_all_submissions_files(**context):
    """Process all submissions JSON files and track extraction vs insertion metrics"""
    try:
        from database.config.config import Config
        from data_pipeline.ingestion.json_extractor_submissions import (
            SECSubmissionsExtractor,
        )

        # Get file paths from the previous task via XCom
        file_paths = context["task_instance"].xcom_pull(
            task_ids="scan_submissions_files"
        )

        if not file_paths:
            print("No submissions files to process from scan task")
            return {
                "files_processed": 0,
                "files_successful": 0,
                "files_failed": 0,
                "inserted_submissions": 0,
            }

        print(f"Processing {len(file_paths)} submissions files from scan task")

        # Convert string paths back to Path objects
        json_files = [Path(f) for f in file_paths]

        # Initialize extractor
        extractor = SECSubmissionsExtractor(json_directory="", db_config=Config())

        # Track processing metrics
        successful_files = 0
        failed_files = 0
        total_records_processed = 0
        failed_file_details = []

        # Process each file individually (similar to test script)
        for i, json_file in enumerate(json_files, 1):
            filename = json_file.name
            print(f"\n[{i}/{len(json_files)}] Processing {filename}...")

            try:
                # Process the single file
                result = extractor.process_single_file(json_file)

                if result["status"] == "success":
                    successful_files += 1
                    total_records_processed += result.get("total_records", 0)
                    print(f"✓ Successfully processed {filename}")
                    print(f"   CIK: {result.get('cik', 'N/A')}")
                    print(f"   Records: {result.get('total_records', 0):,}")
                else:
                    failed_files += 1
                    error_msg = result.get("error", "Unknown error")
                    failed_file_details.append(f"{filename}: {error_msg}")
                    print(f"✗ Failed to process {filename}: {error_msg}")

            except Exception as e:
                failed_files += 1
                error_msg = str(e)
                failed_file_details.append(f"{filename}: {error_msg}")
                print(f"✗ Error processing {filename}: {error_msg}")
                continue

        print(f"\n=== SUBMISSIONS PROCESSING SUMMARY ===")
        print(f"Files Processed: {len(json_files)}")
        print(f"Files Successful: {successful_files}")
        print(f"Files Failed: {failed_files}")
        print(f"Total Records Inserted: {total_records_processed:,}")

        # Show failed files if any
        if failed_files > 0:
            print(f"\n--- Failed Files Details ---")
            for failed_detail in failed_file_details:
                print(f"FAILED: {failed_detail}")

        # Calculate success rate
        success_rate = (successful_files / len(json_files) * 100) if json_files else 0
        print(f"Success Rate: {success_rate:.1f}%")

        # Return metrics via XCom for comparison with database counts
        metrics = {
            "files_processed": len(json_files),
            "files_successful": successful_files,
            "files_failed": failed_files,
            "inserted_submissions": total_records_processed,
            "success_rate": success_rate,
        }

        return metrics

    except Exception as e:
        print(f"Error processing submissions JSON files: {e}")
        import traceback

        traceback.print_exc()
        return {
            "files_processed": 0,
            "files_successful": 0,
            "files_failed": 0,
            "inserted_submissions": 0,
        }


def validate_submissions_setup():
    """Validate that submissions database setup is complete"""
    try:
        from database.db_connection import Session, engine
        from database.models.sec_submissions_raw import BronzeSecSubmissions
        from sqlalchemy import text

        print("=== SUBMISSIONS SETUP VALIDATION ===")

        # Check if table exists
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'bronze_sec_submissions'"
                )
            )
            table_exists = result.scalar() > 0

        if not table_exists:
            print("❌ bronze_sec_submissions table does not exist!")
            print("Run: ./setup_database_submissions.sh")
            raise Exception("Submissions table not found")

        print("✅ bronze_sec_submissions table exists")

        # Check initial record count
        with Session() as session:
            initial_count = session.query(BronzeSecSubmissions).count()
            print(f"✅ Initial submissions count: {initial_count:,}")

        return True

    except Exception as e:
        print(f"❌ Submissions setup validation failed: {e}")
        raise


# Task 1: Validate submissions setup
validate_setup = PythonOperator(
    task_id="validate_submissions_setup",
    python_callable=validate_submissions_setup,
    dag=dag,
)

# Task 2: Scan for submissions JSON files
scan_files = PythonOperator(
    task_id="scan_submissions_files",
    python_callable=scan_submissions_files,
    dag=dag,
)

# Task 3: Process all submissions JSON files
process_submissions = PythonOperator(
    task_id="process_all_submissions_files",
    python_callable=process_all_submissions_files,
    dag=dag,
)

# Task 4: Show final database counts and validation
show_final_counts = PythonOperator(
    task_id="show_final_submissions_counts",
    python_callable=display_submissions_database_counts,
    dag=dag,
)

# Task 5: Simple bash task to show completion
completion_message = BashOperator(
    task_id="submissions_completion_message",
    bash_command='echo "SEC Submissions JSON ingestion completed successfully!"',
    dag=dag,
)

# Define task dependencies
(
    validate_setup
    >> scan_files
    >> process_submissions
    >> show_final_counts
    >> completion_message
)
