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
