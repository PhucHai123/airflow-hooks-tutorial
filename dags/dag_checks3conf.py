import os
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from utils import load_default_args


def check_file_location(file_name: str, **context):
    """Checks if a file exists and logs its location or absence."""
    logger = LoggingMixin().log  # Get a logger instance
    current_directory = os.getcwd()
    file_path = os.path.join(current_directory, file_name)

    if os.path.exists(file_path):
        logger.info(f"File '{file_name}' found at: {file_path}")
    else:
        logger.error(
            f"File '{file_name}' not found in the current directory: {current_directory}"
        )


with DAG(
    dag_id="check_file_location_dag",
    default_args=load_default_args(),
    schedule_interval=None,
    start_date=datetime(2024, 6, 6),
    catchup=False,
) as dag:

    check_s3_config_task = PythonOperator(
        task_id="check_s3_config_file",
        python_callable=check_file_location,
        op_kwargs={"file_name": "s3_config.json"},
    )
