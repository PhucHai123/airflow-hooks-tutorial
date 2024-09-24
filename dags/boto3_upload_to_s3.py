from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils import load_s3_config, load_default_args
import boto3
import os
import logging


def upload_files_to_s3(**context) -> None:
    s3_config = load_s3_config()

    # Get AWS credentials from the environment or configuration
    session = boto3.Session()
    s3_client = session.client("s3")

    # Specify directory path (adjust as needed)
    directory_path = "/opt/airflow/data"

    # List all files in the directory
    for filename in os.listdir(directory_path):
        # Construct the full local path
        local_path = os.path.join(directory_path, filename)

        # Check if it's a file (not a directory)
        if os.path.isfile(local_path):
            # Get the S3 key (you can customize this)
            s3_key = filename

            try:
                s3_client.upload_file(local_path, s3_config["bucket_name"], s3_key)
                logging.info(
                    f"Uploaded {filename} to s3://{s3_config['bucket_name']}/{s3_key}"
                )
            except Exception as e:
                logging.error(f"Error uploading {filename}: {e}")
                raise  # Re-raise the exception to trigger DAG failure


with DAG(
    dag_id="boto3_s3_upload",
    default_args=load_default_args(),
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    upload_task = PythonOperator(
        task_id="upload_files_to_s3",
        python_callable=upload_files_to_s3,
        provide_context=True,  # Provide context to the task
    )
