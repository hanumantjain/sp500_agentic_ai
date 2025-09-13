"""
JSON Data Extractor for SEC Company Facts
Extracts and normalizes data from SEC company facts JSON files
"""

import json
import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import sessionmaker

# Import database models and config
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from database.config.config import Config
from database.models.sec_facts_raw import BronzeSecFacts, BronzeSecFactsDict
from database.db_connection import engine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SECFactsExtractor:
    """Extract and normalize SEC company facts from JSON files"""

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
        Formats CIK as CIK0000000000 (padded to 10 digits).

        Args:
            filepath: Path to JSON file

        Returns:
            Formatted CIK string (e.g., CIK0000001800)
        """
        filename = filepath.stem  # Remove .json extension
        cik_number = filename.replace("CIK", "")
        # Format as CIK0000000000 (padded to 10 digits)
        return f"CIK{int(cik_number):010d}"

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
            logger.error(f"Error loading {filepath.name}: {str(e)}")
            raise

    def normalize_facts_data(
        self, json_data: Dict[str, Any], cik: str
    ) -> List[Dict[str, Any]]:
        """
        Transforms nested SEC company facts JSON into a flat, structured format.
        Returns data ready to be inserted into a relational database.

        Args:
            json_data: Parsed JSON data
            cik: Company CIK

        Returns:
            List of normalized fact records
        """
        normalized_records = []

        try:
            facts = json_data.get("facts", {})

            # Process each taxonomy (dei, us-gaap, etc.)
            for taxonomy, taxonomy_data in facts.items():
                if not isinstance(taxonomy_data, dict):
                    continue

                # Process each tag within taxonomy
                for tag, tag_data in taxonomy_data.items():
                    if not isinstance(tag_data, dict):
                        continue

                    # Extract metadata
                    label = tag_data.get("label", "")
                    description = tag_data.get("description", "")

                    # Store dictionary entry
                    dict_record = {
                        "taxonomy": taxonomy,
                        "tag": tag,
                        "label": label,
                        "description": description,
                    }
                    normalized_records.append(("dict", dict_record))

                    # Process units data
                    units = tag_data.get("units", {})
                    if not isinstance(units, dict):
                        continue

                    for unit_type, unit_data in units.items():
                        if not isinstance(unit_data, list):
                            continue

                        # Process each data point
                        for data_point in unit_data:
                            if not isinstance(data_point, dict):
                                continue

                            # Extract and normalize data
                            fact_record = {
                                "cik": cik,
                                "taxonomy": taxonomy,
                                "tag": tag,
                                "unit": unit_type,
                                "val": self._safe_float(data_point.get("val")),
                                "fy": self._safe_int(data_point.get("fy")),
                                "fp": data_point.get("fp"),
                                "start_date": self._parse_date(data_point.get("start")),
                                "end_date": self._parse_date(data_point.get("end")),
                                "frame": data_point.get("frame"),
                                "form": data_point.get("form"),
                                "filed": self._parse_date(data_point.get("filed")),
                                "accn": data_point.get("accn"),
                            }
                            normalized_records.append(("fact", fact_record))

            return normalized_records

        except Exception as e:
            logger.error(f"Error normalizing data for CIK {cik}: {str(e)}")
            raise

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int"""
        if value is None:
            return None
        try:
            return int(value)
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

    def save_to_database(
        self,
        normalized_records: List[tuple],
        batch_size: int = 1000,
        max_retries: int = 3,
    ):
        """
        Persists normalized records (both facts and tag metadata) into the database.
        Handles batching, insertion, and error handling with retry logic.

        Args:
            normalized_records: List of (record_type, record_data) tuples
            batch_size: Number of records to process in each batch
            max_retries: Maximum number of retry attempts for database operations
        """
        import time

        for attempt in range(max_retries):
            try:
                with self.Session() as session:
                    dict_records = []
                    facts_data = []

                    # Process both dictionary and facts records
                    for record_type, record_data in normalized_records:
                        if record_type == "dict":
                            # Prepare dictionary records for idempotent insert
                            dict_records.append(BronzeSecFactsDict(**record_data))
                        elif record_type == "fact":
                            # Prepare facts data for bulk insert
                            facts_data.append(record_data)

                    # Process dictionary records with simple insert (INSERT ... ON DUPLICATE KEY UPDATE)
                    if dict_records:
                        from sqlalchemy import text
                        from datetime import datetime

                        dict_insert_sql = text(
                            """
                            INSERT INTO bronze_sec_facts_dict 
                            (taxonomy, tag, label, description, created_at, updated_at)
                            VALUES (:taxonomy, :tag, :label, :description, :created_at, :updated_at)
                            ON DUPLICATE KEY UPDATE
                                label = VALUES(label),
                                description = VALUES(description),
                                updated_at = VALUES(updated_at)
                        """
                        )

                        # Insert dictionary records in batches with idempotent logic
                        for i in range(0, len(dict_records), batch_size):
                            batch = dict_records[i : i + batch_size]
                            # Convert ORM objects to dictionaries for raw SQL
                            batch_data = [
                                {
                                    "taxonomy": record.taxonomy,
                                    "tag": record.tag,
                                    "label": record.label,
                                    "description": record.description,
                                    "created_at": datetime.utcnow(),
                                    "updated_at": datetime.utcnow(),
                                }
                                for record in batch
                            ]
                            session.execute(dict_insert_sql, batch_data)
                            session.commit()

                    # Process facts records with raw SQL bulk insert (same as test_sec_fact.py)
                    if facts_data:
                        from sqlalchemy import text

                        insert_sql = text(
                            """
                            INSERT INTO bronze_sec_facts 
                            (cik, taxonomy, tag, unit, val, fy, fp, start_date, end_date, frame, form, filed, accn)
                            VALUES (:cik, :taxonomy, :tag, :unit, :val, :fy, :fp, :start_date, :end_date, :frame, :form, :filed, :accn)
                        """
                        )

                        # Insert facts in batches
                        for i in range(0, len(facts_data), batch_size):
                            batch = facts_data[i : i + batch_size]
                            session.execute(insert_sql, batch)
                            session.commit()

                    return  # Success - exit the retry loop

            except Exception as e:
                logger.warning(
                    f"Database operation failed (attempt {attempt + 1}/{max_retries}): {str(e)}"
                )
                if attempt < max_retries - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"All {max_retries} attempts failed. Database may be unavailable."
                    )
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
            normalized_records = self.normalize_facts_data(json_data, cik)

            # Save to database
            self.save_to_database(normalized_records)

            fact_count = sum(1 for r_type, _ in normalized_records if r_type == "fact")
            dict_count = sum(1 for r_type, _ in normalized_records if r_type == "dict")

            return {
                "cik": cik,
                "fact_records": fact_count,
                "dict_records": dict_count,
                "total_records": len(normalized_records),
                "status": "success",
            }

        except Exception as e:
            logger.error(f"Error processing {filepath.name}: {str(e)}")
            return {
                "cik": cik,
                "fact_records": 0,
                "dict_records": 0,
                "total_records": 0,
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
                logger.error(
                    f"Failed to process {filepath.name}: {result.get('error', 'Unknown error')}"
                )

        # Summary
        successful = sum(1 for r in results if r["status"] == "success")
        total_facts = sum(r["fact_records"] for r in results)
        total_dicts = sum(r["dict_records"] for r in results)

        return results
