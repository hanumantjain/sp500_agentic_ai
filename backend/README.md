Backend
====================================

What this directory contains
- Shell runner for the data pipeline
- Environment setup and dependency lock-in via `requirements.txt`
- Entry point for running normalization from the command line

Setup
------------------------------------
Create and activate a virtual environment, then install dependencies:

```bash
python -m venv sp500
```

```bash
source sp500/bin/activate
```

```bash
pip install -r requirements.txt
```

Run the data pipeline
------------------------------------
See detailed usage in the data pipeline README:

- [data_pipeline/README.md](data_pipeline/README.md)

Notes
------------------------------------
- Input directory: `../data/sp500_ohcl`
- Output directory: `../data/normalised_data`
- Full pipeline documentation: [data_pipeline/README.md](data_pipeline/README.md)