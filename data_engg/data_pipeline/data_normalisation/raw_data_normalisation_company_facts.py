from pathlib import Path
import pandas as pd
import os


class CIKFileExtractor:
    """
    A class to extract files with CIK values from a folder.
    CIK values are loaded from S&P 500 component stocks CSV file.
    """

    def __init__(
        self,
        sp500_csv_path: Path = Path("../data/S_and_P_500_component_stocks.csv"),
        data_source_folder: str = "DATA_SOURCE_FOLDER_PATH",
        output_dir: Path = Path("../data/normalised_data"),
        company_facts_dir: Path = Path("../data/company_facts"),
    ):
        """
        Initialize the CIKFileExtractor.

        Args:
            sp500_csv_path: Path to S&P 500 component stocks CSV file
            data_source_folder: Path to folder containing CIK files (placeholder)
            output_dir: Directory to save extracted file information
            company_facts_dir: Directory to store copied company facts files
        """
        self._sp500_csv_path = sp500_csv_path
        self._data_source_folder = data_source_folder
        self._output_dir = output_dir
        self._company_facts_dir = company_facts_dir
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._company_facts_dir.mkdir(parents=True, exist_ok=True)

        # Storage for CIK values and extracted files
        self._cik_values = []
        self._extracted_files = []
        self._missing_files = []

    def load_cik_values(self):
        """Load CIK values from S&P 500 component stocks CSV file.

        Returns:
            List of CIK values as strings
        """
        try:
            # Read the CSV file with encoding handling
            df = pd.read_csv(self._sp500_csv_path, encoding="latin-1")

            # Extract CIK values and convert to string and pad with zeros to match file format
            self._cik_values = df["CIK"].astype(str).str.zfill(10).tolist()

            return self._cik_values

        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file not found: {self._sp500_csv_path}")
        except Exception as e:
            raise Exception(f"Error loading CIK values: {str(e)}")

    def _match_files_with_cik(self, all_files):
        """Match CIK values with files in the folder.

        Returns:
            Dictionary mapping CIK values to list of matching files
        """
        cik_files_map = {}

        for cik in self._cik_values:
            matching_files = []

            # Look for files with pattern "CIK{cik}.json"
            expected_filename = f"CIK{cik}.json"

            if expected_filename in all_files:
                matching_files.append(expected_filename)
                self._extracted_files.append(expected_filename)
            else:
                self._missing_files.append(expected_filename)

            cik_files_map[cik] = matching_files

        return cik_files_map

    def copy_matched_files(self):
        """Copy matched files to output directory."""
        if not self._extracted_files:
            # Load CIK values if not already loaded
            if not self._cik_values:
                self.load_cik_values()

            # Check if data source folder exists
            if not os.path.exists(self._data_source_folder):
                print(f"Data source folder does not exist: {self._data_source_folder}")
                return []

            # Get all files in the data source folder
            folder_path = Path(self._data_source_folder)
            all_files = [f.name for f in folder_path.iterdir() if f.is_file()]

            # Match files with CIK values
            self._match_files_with_cik(all_files)

        # Use company_facts directory
        dest_path = self._company_facts_dir

        copied_files = []
        for file in self._extracted_files:
            source_file = Path(self._data_source_folder) / file
            dest_file = dest_path / file

            try:
                # Copy file to destination
                import shutil

                shutil.copy2(source_file, dest_file)
                copied_files.append(str(dest_file))
            except Exception as e:
                print(f"Error copying {file}: {str(e)}")

        # Print missing CIK files
        if self._missing_files:
            print(f"\nMissing CIK files ({len(self._missing_files)}):")
            for missing_file in self._missing_files:
                cik = missing_file.replace("CIK", "").replace(".json", "")
                print(f"  - CIK {cik}")

        return copied_files


class CIKsubmissionsExtract:
    """
    A class to extract submission files with CIK values from a folder.
    CIK values are loaded from S&P 500 component stocks CSV file.
    Looks for files with patterns like CIK0000009984-submissions-001, CIK0000009984, etc.
    """

    def __init__(
        self,
        sp500_csv_path: Path = Path("../data/S_and_P_500_component_stocks.csv"),
        data_source_folder: str = "DATA_SOURCE_FOLDER_PATH",
        output_dir: Path = Path("../data/normalised_data"),
        submissions_dir: Path = Path("../data/submissions_facts"),
        batch_size: int = 10000,
    ):
        """
        Initialize the CIKsubmissionsExtract.

        Args:
            sp500_csv_path: Path to S&P 500 component stocks CSV file
            data_source_folder: Path to folder containing CIK submission files
            output_dir: Directory to save extracted file information
            submissions_dir: Directory to store copied submission files
            batch_size: Number of files to process in each batch (default: 10000)
        """
        self._sp500_csv_path = sp500_csv_path
        self._data_source_folder = data_source_folder
        self._output_dir = output_dir
        self._submissions_dir = submissions_dir
        self._batch_size = batch_size
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._submissions_dir.mkdir(parents=True, exist_ok=True)

        # Storage for CIK values and extracted files
        self._cik_values = []
        self._extracted_files = []
        self._missing_files = []

    def load_cik_values(self):
        """Load CIK values from S&P 500 component stocks CSV file.

        Returns:
            List of CIK values as strings
        """
        try:
            # Read the CSV file with encoding handling
            df = pd.read_csv(self._sp500_csv_path, encoding="latin-1")

            # Extract CIK values and convert to string and pad with zeros to match file format
            self._cik_values = df["CIK"].astype(str).str.zfill(10).tolist()

            return self._cik_values

        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file not found: {self._sp500_csv_path}")
        except Exception as e:
            raise Exception(f"Error loading CIK values: {str(e)}")

    def _match_files_with_cik(self, all_files):
        """Match CIK values with submission files in the folder.
        Looks for files with patterns like:
        - CIK0000009984-submissions-001
        - CIK0000009984
        - CIK0000009984-submissions-002
        etc.

        Returns:
            Dictionary mapping CIK values to list of matching files
        """
        cik_files_map = {}

        for cik in self._cik_values:
            matching_files = []

            # Look for files that start with "CIK{cik}"
            cik_prefix = f"CIK{cik}"

            for file in all_files:
                # Check if file starts with the CIK prefix
                if file.startswith(cik_prefix):
                    # Additional checks to ensure it's a submission file
                    # Could be CIK0000009984, CIK0000009984-submissions-001, etc.
                    if (
                        file == cik_prefix
                        or file.startswith(f"{cik_prefix}-submissions-")
                        or file.startswith(f"{cik_prefix}.")
                    ):
                        matching_files.append(file)
                        self._extracted_files.append(file)

            # If no files found for this CIK, add to missing
            if not matching_files:
                self._missing_files.append(cik_prefix)

            cik_files_map[cik] = matching_files

        return cik_files_map

    def _process_file_batch(self, file_batch, batch_num, total_batches):
        """Process a batch of files for CIK matching."""
        import shutil

        print(
            f"Processing batch {batch_num}/{total_batches} ({len(file_batch)} files)..."
        )

        batch_extracted_files = []

        for cik in self._cik_values:
            cik_prefix = f"CIK{cik}"

            for file in file_batch:
                if file.startswith(cik_prefix):
                    if (
                        file == cik_prefix
                        or file.startswith(f"{cik_prefix}-submissions-")
                        or file.startswith(f"{cik_prefix}.")
                    ):
                        batch_extracted_files.append(file)

        # Copy matched files immediately
        copied_files = []
        for file in batch_extracted_files:
            source_file = Path(self._data_source_folder) / file
            dest_file = self._submissions_dir / file

            try:
                shutil.copy2(source_file, dest_file)
                copied_files.append(str(dest_file))
            except Exception as e:
                print(f"Error copying {file}: {str(e)}")

        print(
            f"Batch {batch_num}/{total_batches} completed: {len(copied_files)} files copied"
        )
        return copied_files

    def copy_matched_files(self):
        """Copy matched submission files to output directory using batch processing."""
        # Load CIK values if not already loaded
        if not self._cik_values:
            self.load_cik_values()

        # Check if data source folder exists
        if not os.path.exists(self._data_source_folder):
            print(f"Data source folder does not exist: {self._data_source_folder}")
            return []

        print(f"Starting batch processing with batch size: {self._batch_size}")

        # Use submissions_facts directory
        dest_path = self._submissions_dir
        folder_path = Path(self._data_source_folder)

        all_copied_files = []
        batch_num = 0
        current_batch = []

        # Count total files first for progress tracking
        print("Counting total files...")
        total_files = sum(1 for f in folder_path.iterdir() if f.is_file())
        total_batches = (total_files + self._batch_size - 1) // self._batch_size
        print(f"Total files to process: {total_files:,}")
        print(
            f"Processing in {total_batches} batches of {self._batch_size:,} files each"
        )
        print("")

        # Process files in batches
        for file_path in folder_path.iterdir():
            if file_path.is_file():
                current_batch.append(file_path.name)

                # Process batch when it reaches batch_size
                if len(current_batch) >= self._batch_size:
                    batch_num += 1
                    batch_copied = self._process_file_batch(
                        current_batch, batch_num, total_batches
                    )
                    all_copied_files.extend(batch_copied)
                    current_batch = []  # Reset batch

        # Process remaining files in the last batch
        if current_batch:
            batch_num += 1
            batch_copied = self._process_file_batch(
                current_batch, batch_num, total_batches
            )
            all_copied_files.extend(batch_copied)

        # Check for missing CIKs
        found_ciks = set()
        for file in all_copied_files:
            filename = Path(file).name
            if filename.startswith("CIK"):
                # Extract CIK from filename
                cik_part = filename.split("-")[0].replace("CIK", "")
                if cik_part:
                    found_ciks.add(cik_part)

        missing_ciks = []
        for cik in self._cik_values:
            if cik not in found_ciks:
                missing_ciks.append(cik)

        # Print summary
        print(f"\n{'='*50}")
        print(f"BATCH PROCESSING COMPLETED")
        print(f"{'='*50}")
        print(f"Total files copied: {len(all_copied_files):,}")
        print(f"CIKs found: {len(found_ciks)}/{len(self._cik_values)}")

        if missing_ciks:
            print(f"\nMissing CIK submission files ({len(missing_ciks)}):")
            for missing_cik in missing_ciks[:10]:  # Show only first 10
                print(f"  - CIK {missing_cik}")
            if len(missing_ciks) > 10:
                print(f"  ... and {len(missing_ciks) - 10} more")

        return all_copied_files
