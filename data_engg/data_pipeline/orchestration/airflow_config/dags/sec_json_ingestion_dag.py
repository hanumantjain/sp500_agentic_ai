"""
SEC JSON Ingestion DAG
Simple DAG that replicates the test_json_to_db.py functionality
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import sys
from pathlib import Path

# Add data_engg directory to Python path for imports
data_engg_root = Path(__file__).parent.parent.parent.parent.parent
if str(data_engg_root) not in sys.path:
    sys.path.insert(0, str(data_engg_root))

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
    "sec_json_ingestion",
    default_args=default_args,
    description="Process all SEC JSON files and load to database",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["sec", "json", "ingestion", "batch"],
)


def display_database_counts(**context):
    """Display database counts and compare with processing metrics"""
    try:
        from database.db_connection import Session
        from database.models.sec_facts_raw import BronzeSecFacts, BronzeSecFactsDict

        # Get processing metrics from previous task
        processing_metrics = context["task_instance"].xcom_pull(
            task_ids="process_all_json_files"
        )

        with Session() as session:
            # Count records in bronze_sec_facts table
            db_facts_count = session.query(BronzeSecFacts).count()

            # Count records in bronze_sec_facts_dict table
            db_dict_count = session.query(BronzeSecFactsDict).count()
            db_total_count = db_facts_count + db_dict_count

            print("\n=== DATABASE vs PROCESSING COMPARISON ===")
            print(
                f"Files Processed: {processing_metrics.get('files_processed', 'N/A')}"
            )
            print(
                f"Files Successful: {processing_metrics.get('files_successful', 'N/A')}"
            )
            print(f"Files Failed: {processing_metrics.get('files_failed', 'N/A')}")
            print()

            print("FACTS TABLE:")
            print(
                f"   Processing Inserted: {processing_metrics.get('inserted_facts', 0):,}"
            )
            print(f"   Database Count:      {db_facts_count:,}")
            facts_diff = db_facts_count - processing_metrics.get("inserted_facts", 0)
            print(f"   Difference:          {facts_diff:,}")
            if processing_metrics.get("inserted_facts", 0) > 0:
                facts_match = "MATCH" if facts_diff == 0 else "MISMATCH"
                print(f"   Status:              {facts_match}")
            print()

            print("DICTIONARY TABLE:")
            print(
                f"   Processing Inserted: {processing_metrics.get('inserted_dicts', 0):,}"
            )
            print(f"   Database Count:      {db_dict_count:,}")
            dict_diff = db_dict_count - processing_metrics.get("inserted_dicts", 0)
            print(f"   Difference:          {dict_diff:,}")
            if processing_metrics.get("inserted_dicts", 0) > 0:
                dict_match = "MATCH" if dict_diff == 0 else "MISMATCH"
                print(f"   Status:              {dict_match}")
            print()

            print("TOTAL RECORDS:")
            print(
                f"   Processing Inserted: {processing_metrics.get('total_inserted', 0):,}"
            )
            print(f"   Database Count:      {db_total_count:,}")
            total_diff = db_total_count - processing_metrics.get("total_inserted", 0)
            print(f"   Difference:          {total_diff:,}")
            if processing_metrics.get("total_inserted", 0) > 0:
                total_match = "MATCH" if total_diff == 0 else "MISMATCH"
                print(f"   Status:              {total_match}")

            # Show sample CIK values if any records exist
            if db_facts_count > 0:
                sample_ciks = (
                    session.query(BronzeSecFacts.cik).distinct().limit(5).all()
                )
                cik_list = [cik[0] for cik in sample_ciks]
                print(f"\nSample CIK values in database: {cik_list}")

            return db_facts_count, db_dict_count

    except Exception as e:
        print(f"Error querying database: {e}")
        return 0, 0


def scan_json_files():
    """Scan and return list of all JSON files in company_facts directory"""
    try:
        # Path to the JSON files directory (relative to project root)
        project_root = backend_root.parent
        json_directory = project_root / "data" / "company_facts"
        json_dir = Path(json_directory)

        if not json_dir.exists():
            print(f"JSON directory not found: {json_directory}")
            return []

        # Find all JSON files
        json_files = list(json_dir.glob("*.json"))

        if not json_files:
            print(f"No JSON files found in {json_directory}")
            return []

        print(f"Found {len(json_files)} JSON files to process")
        print(f"First 5 files: {[f.name for f in json_files[:5]]}")
        print(f"Last 5 files: {[f.name for f in json_files[-5:]]}")

        # Return file paths as strings for XCom
        file_paths = [str(f) for f in json_files]
        return file_paths

    except Exception as e:
        print(f"Error scanning JSON files: {e}")
        import traceback

        traceback.print_exc()
        return []


def process_all_json_files(**context):
    """Process all JSON files and track extraction vs insertion metrics"""
    try:
        from database.config.config import Config
        from data_pipeline.ingestion.json_extractor import SECFactsExtractor

        # Get file paths from the previous task via XCom
        file_paths = context["task_instance"].xcom_pull(task_ids="scan_json_files")

        if not file_paths:
            print("No files to process from scan task")
            return False

        print(f"Processing {len(file_paths)} files from scan task")

        # Convert string paths back to Path objects
        json_files = [Path(f) for f in file_paths]

        # Initialize extractor
        extractor = SECFactsExtractor(json_directory="", db_config=Config())

        # Track extraction metrics
        total_extracted_facts = 0
        total_extracted_dicts = 0
        total_extracted_records = 0

        # Process all files as a batch
        results = extractor.process_json_batch(json_files)

        # Calculate extraction vs insertion metrics
        successful = sum(1 for r in results if r["status"] == "success")
        failed = len(results) - successful

        # Total records extracted (from all files, successful or not)
        total_extracted_facts = sum(r["fact_records"] for r in results)
        total_extracted_dicts = sum(r["dict_records"] for r in results)
        total_extracted_records = total_extracted_facts + total_extracted_dicts

        # Total records successfully inserted (only from successful files)
        total_inserted_facts = sum(
            r["fact_records"] for r in results if r["status"] == "success"
        )
        total_inserted_dicts = sum(
            r["dict_records"] for r in results if r["status"] == "success"
        )
        total_inserted_records = total_inserted_facts + total_inserted_dicts

        print(f"\n=== EXTRACTION vs INSERTION METRICS ===")
        print(f"Files Processed: {len(json_files)}")
        print(f"Files Successful: {successful}")
        print(f"Files Failed: {failed}")
        print()
        print(f"RECORD COUNTS:")
        print(f"   Extracted Facts: {total_extracted_facts:,}")
        print(f"   Inserted Facts:  {total_inserted_facts:,}")
        print(
            f"   Facts Success Rate: {(total_inserted_facts/total_extracted_facts*100):.1f}%"
            if total_extracted_facts > 0
            else "   Facts Success Rate: N/A"
        )
        print()
        print(f"   Extracted Dicts: {total_extracted_dicts:,}")
        print(f"   Inserted Dicts:  {total_inserted_dicts:,}")
        print(
            f"   Dicts Success Rate: {(total_inserted_dicts/total_extracted_dicts*100):.1f}%"
            if total_extracted_dicts > 0
            else "   Dicts Success Rate: N/A"
        )
        print()
        print(f"   Total Extracted: {total_extracted_records:,}")
        print(f"   Total Inserted:  {total_inserted_records:,}")
        print(
            f"   Overall Success Rate: {(total_inserted_records/total_extracted_records*100):.1f}%"
            if total_extracted_records > 0
            else "   Overall Success Rate: N/A"
        )

        # Show failed files if any
        if failed > 0:
            print(f"\n--- Failed Files ---")
            for result in results:
                if result["status"] != "success":
                    print(
                        f"FAILED {result['cik']}: {result.get('error', 'Unknown error')}"
                    )

        # Return metrics via XCom for comparison with database counts
        metrics = {
            "files_processed": len(json_files),
            "files_successful": successful,
            "files_failed": failed,
            "extracted_facts": total_extracted_facts,
            "inserted_facts": total_inserted_facts,
            "extracted_dicts": total_extracted_dicts,
            "inserted_dicts": total_inserted_dicts,
            "total_extracted": total_extracted_records,
            "total_inserted": total_inserted_records,
        }

        return metrics

    except Exception as e:
        print(f"Error processing JSON files: {e}")
        import traceback

        traceback.print_exc()
        return False


# Task 1: Scan for JSON files
scan_files = PythonOperator(
    task_id="scan_json_files",
    python_callable=scan_json_files,
    dag=dag,
)

# Task 2: Process all JSON files with extraction vs insertion metrics
process_json = PythonOperator(
    task_id="process_all_json_files",
    python_callable=process_all_json_files,
    dag=dag,
)

# Task 3: Show final database counts
show_final_counts = PythonOperator(
    task_id="show_final_counts",
    python_callable=display_database_counts,
    dag=dag,
)

# Task 4: Simple bash task to show completion
completion_message = BashOperator(
    task_id="completion_message",
    bash_command='echo "SEC JSON ingestion completed successfully!"',
    dag=dag,
)

# Define task dependencies
scan_files >> process_json >> show_final_counts >> completion_message
