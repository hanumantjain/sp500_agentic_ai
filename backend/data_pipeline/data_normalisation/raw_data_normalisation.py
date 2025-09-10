from pathlib import Path
import pandas as pd
from datetime import datetime


class ExtractFileName:
    def __init__(
        self,
        input_dir: Path = Path("../data/sp500_ohcl"),
        output_dir: Path = Path("../data/normalised_data"),
    ):
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._file_names = []

    def get_file_names(self):
        """Extract file names (with extensions) from input directory."""
        self._file_names = [
            file.name
            for file in self._input_dir.iterdir()
            if file.is_file() and not file.name.startswith(".")
        ]
        return self._file_names

    def save_to_csv(self, filename="file_names.csv"):
        """Save complete file names to CSV."""
        if not self._file_names:
            self.get_file_names()

        df = pd.DataFrame(self._file_names, columns=["file_name"])
        output_file = self._output_dir / filename
        df.to_csv(output_file, index=False)

        print(f"Saved {len(self._file_names)} complete file names to {output_file}")
        return output_file


class DataNormalizer:
    def __init__(
        self,
        input_dir: Path = Path("../data/sp500_ohcl"),
        output_dir: Path = Path("../data/normalised_data"),
        start_date: str = None,
        end_date: str = None,
    ):
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._normalized_data = []
        self._start_date = start_date
        self._end_date = end_date

    def normalize_single_file(self, filename: str):
        """Normalize a single txt file and return DataFrame with required columns."""
        file_path = self._input_dir / filename

        if not file_path.exists():
            print(f"Warning: File {filename} not found")
            return None

        try:
            # Read the txt file
            df = pd.read_csv(file_path, sep=",")

            # Create normalized DataFrame with only required columns
            normalized_df = pd.DataFrame(
                {
                    "Ticker": df["<TICKER>"],
                    "Date": pd.to_datetime(df["<DATE>"], format="%Y%m%d").dt.strftime(
                        "%Y-%m-%d"
                    ),
                    "Open": df["<OPEN>"],
                    "High": df["<HIGH>"],
                    "Low": df["<LOW>"],
                    "Close": df["<CLOSE>"],
                    "Volume": df["<VOL>"],
                }
            )

            # Apply date filtering if specified
            if self._start_date or self._end_date:
                normalized_df = self._apply_date_filter(normalized_df)

            return normalized_df

        except Exception as e:
            print(f"Error processing {filename}: {e}")
            return None

    def _apply_date_filter(self, df):
        """Apply date filtering to DataFrame based on start_date and end_date."""
        if df.empty:
            return df

        # Convert Date column to datetime for comparison
        df["Date"] = pd.to_datetime(df["Date"])

        # Apply start date filter
        if self._start_date:
            start_date = pd.to_datetime(self._start_date)
            df = df[df["Date"] >= start_date]

        # Apply end date filter
        if self._end_date:
            end_date = pd.to_datetime(self._end_date)
            df = df[df["Date"] <= end_date]

        # Convert back to string format
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

        return df

    def normalize_all_files(self, file_names_csv="file_names.csv", batch_size=50):
        """Normalize all txt files using batch processing to reduce memory usage."""
        # Read the file names from CSV
        file_names_path = self._output_dir / file_names_csv

        if not file_names_path.exists():
            print(f"Error: File names CSV {file_names_csv} not found")
            print("Please run ExtractFileName first to generate the file names list")
            return None

        try:
            file_names_df = pd.read_csv(file_names_path)
            file_names = file_names_df["file_name"].tolist()
            print(
                f"Found {len(file_names)} files to normalize from {file_names_csv}..."
            )
        except Exception as e:
            print(f"Error reading file names CSV: {e}")
            return None

        # Initialize output file
        output_file = self._output_dir / "normalized_sp500_data.csv"

        processed_count = 0
        failed_count = 0
        total_records = 0
        batch_data = []
        is_first_batch = True

        for i, filename in enumerate(file_names):
            normalized_df = self.normalize_single_file(filename)
            if normalized_df is not None:
                batch_data.append(normalized_df)
                processed_count += 1
            else:
                failed_count += 1

            # Process batch when it reaches batch_size or at the end
            if len(batch_data) >= batch_size or i == len(file_names) - 1:
                if batch_data:
                    # Combine batch DataFrames
                    batch_combined = pd.concat(batch_data, ignore_index=True)

                    # Write batch to CSV (append mode after first batch)
                    mode = "w" if is_first_batch else "a"
                    header = is_first_batch
                    batch_combined.to_csv(
                        output_file, mode=mode, header=header, index=False
                    )

                    total_records += len(batch_combined)
                    print(
                        f"Processed batch: {len(batch_data)} files, {len(batch_combined)} records"
                    )

                    # Clear batch data to free memory
                    batch_data = []
                    is_first_batch = False

        print(f"Successfully normalized {processed_count} files")
        if failed_count > 0:
            print(f"Failed to process {failed_count} files")
        print(f"Total records: {total_records}")
        print(f"Data saved to: {output_file}")

        # Store summary for return
        self._normalized_data = pd.read_csv(output_file) if total_records > 0 else None
        return self._normalized_data

    def save_normalized_data(self, filename="normalized_sp500_data.csv"):
        """Save normalized data to CSV."""
        if self._normalized_data is None or self._normalized_data.empty:
            print("No normalized data available. Run normalize_all_files() first.")
            return None

        output_file = self._output_dir / filename

        # Create backup if file exists
        if output_file.exists():
            backup_file = output_file.with_suffix(".csv.backup")
            output_file.rename(backup_file)
            print(f"Backed up existing file to {backup_file}")

        # Save to CSV
        self._normalized_data.to_csv(output_file, index=False)
        print(f"Saved {len(self._normalized_data)} normalized records to {output_file}")
        return output_file


class DataPipelinenormalization:
    """Complete data pipeline that runs both file extraction and normalization."""

    def __init__(
        self,
        input_dir: Path = Path("../data/sp500_ohcl"),
        output_dir: Path = Path("../data/normalised_data"),
        start_date: str = None,
        end_date: str = None,
        batch_size: int = 50,
    ):
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._start_date = start_date
        self._end_date = end_date
        self._batch_size = batch_size
        self._file_extractor = ExtractFileName(input_dir, output_dir)
        self._data_normalizer = DataNormalizer(
            input_dir, output_dir, start_date, end_date
        )

    def run_complete_pipeline(self):
        """Run the complete data pipeline: extract file names and normalize data."""
        print("=" * 60)
        print("Starting Complete S&P 500 Data Pipeline")
        print("=" * 60)

        # Show date range if specified
        if self._start_date or self._end_date:
            print(f"Date Range Filter:")
            print(
                f"- Start Date: {self._start_date if self._start_date else 'Not specified'}"
            )
            print(
                f"- End Date: {self._end_date if self._end_date else 'Not specified'}"
            )
            print("-" * 60)

        # Step 1: Extract file names
        print("\nStep 1: Extracting file names...")
        file_names = self._file_extractor.get_file_names()
        file_csv_path = self._file_extractor.save_to_csv()
        print(f"Extracted {len(file_names)} file names")

        # Step 2: Normalize all data using batch processing
        print(
            f"\nStep 2: Normalizing data from all files (batch size: {self._batch_size})..."
        )
        normalized_data = self._data_normalizer.normalize_all_files(
            batch_size=self._batch_size
        )

        if normalized_data is not None:
            print("\n" + "=" * 60)
            print("Pipeline completed successfully!")
            print("=" * 60)

            return {
                "file_names_count": len(file_names),
                "normalized_records": len(normalized_data),
                "output_files": [
                    file_csv_path,
                    str(self._output_dir / "normalized_sp500_data.csv"),
                ],
            }
        else:
            print("\nPipeline failed during data normalization")
            return None
