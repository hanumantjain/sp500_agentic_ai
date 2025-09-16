"""
JSON Data Extractor for SEC Submissions Facts
Extracts and normalizes data from SEC submissions facts JSON files
"""

import json
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import sessionmaker

# Import database models and config
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from database.config.config import Config
from database.db_connection import engine


class SECSubmissionsExtractor:
    """Extract and normalize SEC submissions facts from JSON files"""

    def __init__(self, json_directory: str, db_config: Config):
        """
        Initialize the extractor

        Args:
            json_directory: Path to directory containing JSON files
            db_config: Database configuration object
        """
        self.json_directory = Path(json_directory)
        self.db_config = db_config
        self.engine = engine
        self.Session = sessionmaker(bind=self.engine)

    def scan_json_files(self) -> List[Path]:
        """
        Scans a given directory for all JSON files.
        Primarily used in CLI or test runs to find which files to process.

        Returns:
            List of Path objects for JSON files
        """
        json_files = list(self.json_directory.glob("*.json"))
        return json_files

    def extract_cik_from_filename(self, filepath: Path) -> str:
        """
        Extracts the company CIK identifier from a JSON filename.
        Ensures that each data record is associated with the correct company.
        Formats CIK as 0000000000 (padded to 10 digits without CIK prefix).

        Handles both formats:
        - CIK0000001800.json
        - CIK0000001800-submissions-001.json

        Args:
            filepath: Path to JSON file

        Returns:
            Formatted CIK string (e.g., 0000001800)
        """
        filename = filepath.stem  # Remove .json extension
        cik_part = filename.replace("CIK", "")

        # Handle submissions files like CIK0000001800-submissions-001
        if "-submissions-" in cik_part:
            cik_number = cik_part.split("-submissions-")[0]
        else:
            cik_number = cik_part

        # Format as 0000000000 (padded to 10 digits)
        return f"{int(cik_number):010d}"

    def load_json_file(self, filepath: Path) -> Dict[str, Any]:
        """
        Reads and parses a single JSON file into memory.
        Handles file I/O and prepares the raw data for normalization.

        Args:
            filepath: Path to JSON file

        Returns:
            Parsed JSON data
        """
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data
        except Exception as e:
            print(f"Error loading {filepath.name}: {str(e)}")
            raise

    def normalize_submissions_data(
        self, json_data: Dict[str, Any], cik: str
    ) -> List[Dict[str, Any]]:
        """
        Transforms SEC submissions JSON into a flat, structured format.
        Handles both nested (filings.recent) and direct array structures.
        Returns only filing data with CIK.

        Args:
            json_data: Parsed JSON data
            cik: Company CIK

        Returns:
            List of normalized filing records
        """
        normalized_records = []

        try:
            # Determine structure and extract data
            data_source = None

            # Check for nested structure (main files)
            filings = json_data.get("filings", {})
            if filings:
                recent = filings.get("recent", {})
                if recent:
                    data_source = recent

            # Check for direct structure (submissions-001/002 files)
            elif "accessionNumber" in json_data:
                data_source = json_data

            if data_source:
                # Get all filing arrays from the data source
                accession_numbers = data_source.get("accessionNumber", [])
                filing_dates = data_source.get("filingDate", [])
                report_dates = data_source.get("reportDate", [])
                acceptance_datetimes = data_source.get("acceptanceDateTime", [])
                acts = data_source.get("act", [])
                forms = data_source.get("form", [])
                file_numbers = data_source.get("fileNumber", [])
                film_numbers = data_source.get("filmNumber", [])
                items = data_source.get("items", [])
                sizes = data_source.get("size", [])
                is_xbrl = data_source.get("isXBRL", [])
                is_inline_xbrl = data_source.get("isInlineXBRL", [])
                primary_documents = data_source.get("primaryDocument", [])
                primary_doc_descriptions = data_source.get("primaryDocDescription", [])

                # Create filing records
                max_length = max(
                    len(arr)
                    for arr in [
                        accession_numbers,
                        filing_dates,
                        report_dates,
                        acceptance_datetimes,
                        acts,
                        forms,
                        file_numbers,
                        film_numbers,
                        items,
                        sizes,
                        is_xbrl,
                        is_inline_xbrl,
                        primary_documents,
                        primary_doc_descriptions,
                    ]
                    if arr
                )

                for i in range(max_length):
                    filing_record = {
                        "cik": cik,
                        "accession_number": self._safe_get(accession_numbers, i),
                        "filing_date": self._parse_date(
                            self._safe_get(filing_dates, i)
                        ),
                        "report_date": self._parse_date(
                            self._safe_get(report_dates, i)
                        ),
                        "acceptance_datetime": self._parse_datetime(
                            self._safe_get(acceptance_datetimes, i)
                        ),
                        "act": self._safe_get(acts, i),
                        "form": self._safe_get(forms, i),
                        "file_number": self._safe_get(file_numbers, i),
                        "film_number": self._safe_numeric(
                            self._safe_get(film_numbers, i)
                        ),
                        "items": self._safe_get(items, i),
                        "size": self._safe_int(self._safe_get(sizes, i)),
                        "is_xbrl": self._safe_int(self._safe_get(is_xbrl, i)),
                        "is_inline_xbrl": self._safe_int(
                            self._safe_get(is_inline_xbrl, i)
                        ),
                        "primary_document": self._safe_get(primary_documents, i),
                        "primary_doc_description": self._safe_get(
                            primary_doc_descriptions, i
                        ),
                    }
                    normalized_records.append(("filing", filing_record))

            return normalized_records

        except Exception as e:
            print(f"Error normalizing submissions data for CIK {cik}: {str(e)}")
            raise

    def _safe_get(self, arr: List[Any], index: int) -> Optional[Any]:
        """Safely get item from array at index"""
        try:
            return arr[index] if index < len(arr) else None
        except (IndexError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int"""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _safe_numeric(self, value: Any) -> Optional[float]:
        """Safely convert value to numeric (for database Numeric fields), return None if conversion fails"""
        if value is None or value == "":
            return None
        try:
            # Try to convert to int first, then float
            if isinstance(value, (int, float)):
                return float(value)
            # Handle string values
            if isinstance(value, str):
                if value.strip() == "":
                    return None
                # Try integer first
                if "." not in value:
                    return float(int(value))
                else:
                    return float(value)
            return None
        except (ValueError, TypeError):
            return None

    def _parse_date(self, date_str: Any) -> Optional[datetime]:
        """Parse date string to datetime object"""
        if not date_str:
            return None
        try:
            if isinstance(date_str, str):
                return datetime.strptime(date_str, "%Y-%m-%d")
            return date_str
        except (ValueError, TypeError):
            return None

    def _parse_datetime(self, datetime_str: Any) -> Optional[datetime]:
        """Parse datetime string to datetime object"""
        if not datetime_str:
            return None
        try:
            if isinstance(datetime_str, str):
                # Handle ISO format with Z suffix
                if datetime_str.endswith("Z"):
                    datetime_str = datetime_str[:-1] + "+00:00"
                return datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
            return datetime_str
        except (ValueError, TypeError):
            return None

    def save_to_database(
        self,
        normalized_records: List[tuple],
        batch_size: int = 10000,
        max_retries: int = 3,
    ):
        """
        Persists normalized records into the database.

        Args:
            normalized_records: List of (record_type, record_data) tuples
            batch_size: Number of records to process in each batch
            max_retries: Maximum number of retry attempts for database operations
        """
        if not self.db_config:
            print("Database save not implemented yet for submissions data")
            print(f"Would save {len(normalized_records)} records to database")

            # Count records by type
            record_counts = {}
            for record_type, _ in normalized_records:
                record_counts[record_type] = record_counts.get(record_type, 0) + 1

            for record_type, count in record_counts.items():
                print(f"  {record_type}: {count} records")
            return

        try:
            from database.db_connection import Session
            from database.models.sec_submissions_raw import BronzeSecSubmissions
            from sqlalchemy import text

            print(f"Saving {len(normalized_records)} records to database")

            # Separate records by type
            filing_records = []
            for record_type, record_data in normalized_records:
                if record_type == "filing":
                    filing_records.append(record_data)

            if filing_records:
                print(f"Inserting {len(filing_records)} filing records...")

                with Session() as session:
                    try:
                        # Insert filing records in batches
                        for i in range(0, len(filing_records), batch_size):
                            batch = filing_records[i : i + batch_size]

                            # Convert to SQLAlchemy objects
                            submission_objects = []

                            for record in batch:
                                submission_obj = BronzeSecSubmissions(
                                    cik=record["cik"],
                                    accession_number=record["accession_number"],
                                    filing_date=record["filing_date"],
                                    report_date=record["report_date"],
                                    acceptance_datetime=record["acceptance_datetime"],
                                    act=record["act"],
                                    form=record["form"],
                                    file_number=record["file_number"],
                                    film_number=record["film_number"],
                                    items=record["items"],
                                    size=record["size"],
                                    is_xbrl=record["is_xbrl"],
                                    is_inline_xbrl=record["is_inline_xbrl"],
                                    primary_document=record["primary_document"],
                                    primary_doc_description=record[
                                        "primary_doc_description"
                                    ],
                                )
                                submission_objects.append(submission_obj)

                            # Use bulk INSERT ... ON DUPLICATE KEY UPDATE for idempotency
                            stmt = text(
                                """
                                INSERT INTO bronze_sec_submissions 
                                (cik, accession_number, filing_date, acceptance_datetime, 
                                 report_date, act, form, file_number, film_number, items, 
                                 size, is_xbrl, is_inline_xbrl, primary_document, 
                                 primary_doc_description)
                                VALUES 
                                (:cik, :accession_number, :filing_date, :acceptance_datetime,
                                 :report_date, :act, :form, :file_number, :film_number, :items,
                                 :size, :is_xbrl, :is_inline_xbrl, :primary_document,
                                 :primary_doc_description)
                                ON DUPLICATE KEY UPDATE
                                    report_date = VALUES(report_date),
                                    act = VALUES(act),
                                    form = VALUES(form),
                                    file_number = VALUES(file_number),
                                    film_number = VALUES(film_number),
                                    items = VALUES(items),
                                    size = VALUES(size),
                                    is_xbrl = VALUES(is_xbrl),
                                    is_inline_xbrl = VALUES(is_inline_xbrl),
                                    primary_document = VALUES(primary_document),
                                    primary_doc_description = VALUES(primary_doc_description)
                            """
                            )

                            # Prepare bulk data for executemany
                            bulk_data = []
                            for submission_obj in submission_objects:
                                bulk_data.append(
                                    {
                                        "cik": submission_obj.cik,
                                        "accession_number": submission_obj.accession_number,
                                        "filing_date": submission_obj.filing_date,
                                        "acceptance_datetime": submission_obj.acceptance_datetime,
                                        "report_date": submission_obj.report_date,
                                        "act": submission_obj.act,
                                        "form": submission_obj.form,
                                        "file_number": submission_obj.file_number,
                                        "film_number": submission_obj.film_number,
                                        "items": submission_obj.items,
                                        "size": submission_obj.size,
                                        "is_xbrl": submission_obj.is_xbrl,
                                        "is_inline_xbrl": submission_obj.is_inline_xbrl,
                                        "primary_document": submission_obj.primary_document,
                                        "primary_doc_description": submission_obj.primary_doc_description,
                                    }
                                )

                            # Execute bulk insert with ON DUPLICATE KEY UPDATE
                            session.execute(stmt, bulk_data)

                            session.commit()
                            print(
                                f"Processed batch {i//batch_size + 1}: {len(batch)} records (insert/update)"
                            )

                    except Exception as e:
                        session.rollback()
                        print(f"Error inserting submissions records: {e}")
                        raise

            print(f"Successfully saved {len(normalized_records)} records to database")

        except ImportError as e:
            print(f"Database modules not available: {e}")
            print("Skipping database save")
        except Exception as e:
            print(f"Error saving to database: {e}")
            raise

    def process_single_file(self, filepath: Path) -> Dict[str, int]:
        """
        Runs the full processing pipeline for one file: extract CIK → load JSON → normalize → save to DB.
        Returns statistics about success or failure and number of records processed.

        Args:
            filepath: Path to JSON file

        Returns:
            Dictionary with processing statistics
        """
        cik = self.extract_cik_from_filename(filepath)

        try:
            # Load JSON data
            json_data = self.load_json_file(filepath)

            # Normalize data
            normalized_records = self.normalize_submissions_data(json_data, cik)

            # Save to database (currently just logs)
            self.save_to_database(normalized_records)

            # Count records by type
            record_counts = {}
            submission_records = 0
            for record_type, _ in normalized_records:
                record_counts[record_type] = record_counts.get(record_type, 0) + 1
                if record_type == "filing":
                    submission_records += 1

            return {
                "cik": cik,
                "submission_records": submission_records,
                "total_records": len(normalized_records),
                "record_counts": record_counts,
                "status": "success",
            }

        except Exception as e:
            print(f"Error processing {filepath.name}: {str(e)}")
            return {
                "cik": cik,
                "submission_records": 0,
                "total_records": 0,
                "record_counts": {},
                "status": "error",
                "error": str(e),
            }

    def process_json_batch(self, filepaths: List[Path]) -> List[Dict[str, int]]:
        """
        Processes a list of file paths as a batch.
        Loops through each file and calls the full single-file pipeline for each.
        Used in batch-based orchestration (e.g. Airflow).

        Args:
            filepaths: List of Path objects for JSON files to process

        Returns:
            List of processing statistics for each file
        """
        results = []

        for filepath in filepaths:
            result = self.process_single_file(filepath)
            results.append(result)

            # Log progress
            if result["status"] != "success":
                print(
                    f"Failed to process {filepath.name}: {result.get('error', 'Unknown error')}"
                )

        # Summary
        successful = sum(1 for r in results if r["status"] == "success")
        total_records = sum(r["total_records"] for r in results)

        print(
            f"Processed {len(results)} files: {successful} successful, {total_records} total records"
        )

        return results
