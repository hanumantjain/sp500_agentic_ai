Data Pipeline
====================================

Purpose
- Extract filenames from the input directory and normalize price data
- Merge data into a single CSV for downstream use

How it works
- Input: reads `.txt` files from `../data/sp500_ohcl`
- Output: writes to `../data/normalised_data/normalized_sp500_data.csv`
- Keeps columns: `Ticker, Date, Open, High, Low, Close, Volume`
- Converts `Date` to `YYYY-MM-DD`
- Optional date filtering via shell script arguments

Run
From the `backend/` directory:

```bash
./run_data_normalization.sh [start_date] [end_date]
```

Examples:

```bash
# Process all files (no date filter)
./run_data_normalization.sh
```

```bash
./run_data_normalization.sh "2023-01-01" "2023-12-31"
```

```bash
# End-date only (up to a date)
./run_data_normalization.sh "" "2023-12-31"
```

Notes
- Batch processing reduces memory usage
- Hidden files are ignored during filename extraction

