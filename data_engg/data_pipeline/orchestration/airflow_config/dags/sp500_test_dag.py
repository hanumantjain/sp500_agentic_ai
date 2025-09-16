from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    "owner": "ssp",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 13),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    "sp500_data_pipeline_test",
    default_args=default_args,
    description="Test DAG for SP500 data pipeline",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["sp500", "test", "data-pipeline"],
)


# Define tasks
def test_python_task():
    """Test Python task"""
    print("SP500 Data Pipeline Test - Python Task Executed Successfully!")
    return "Python task completed"


# Bash task
bash_task = BashOperator(
    task_id="test_bash_task",
    bash_command='echo "SP500 Data Pipeline Test - Bash Task Executed Successfully!"',
    dag=dag,
)

# Python task
python_task = PythonOperator(
    task_id="test_python_task",
    python_callable=test_python_task,
    dag=dag,
)

# Set task dependencies
bash_task >> python_task
