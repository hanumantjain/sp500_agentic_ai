# Backend

## Create a virtual environment and Install dependancies

### Create a virtual environment

python -m venv sp500

### Activate the virtual environment

source sp500/bin/activate

### Install packages

pip install -r requirements.txt


### Process last 3 years of data
./run_data_normalization.sh "2021-01-01" "2023-12-31"

### Process specific year
./run_data_normalization.sh "2023-01-01" "2023-12-31"

### Process all data (no filtering)
./run_data_normalization.sh