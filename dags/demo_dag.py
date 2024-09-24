# load necessary binaries, packages
import os
import logging
import json

from datetime import datetime
from airflow import DAG, Dataset
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils import load_default_args, load_s3_config

# Create and save logs
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(f"{log_dir}/s3_demo.log"), logging.StreamHandler()],
)


"""
This is the implementation of a dag script for Airflow Hooks demo.
"""

"""
bucket_name = "airflow-hook-test/"
file_name = "mock_data.csv"
aws_conn_id = "airflow-s3hook-demo"
"""


# Custom Operator
class CustomOperator(PythonOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(python_callable=self.my_custom_function, *args, **kwargs)

    def custom_operator(self, **kwargs) -> str:
        logging.info("This is a custom operator.")
        return "Custom Operator Tested!"


with DAG(
    dag_id="demo_hook_test",
    default_args=load_default_args(),
    description="A simple demo DAG for Hook Demo Tests",
    schedule_interval=None,
    start_date=datetime(2024, 6, 6),
    end_date=datetime(2024, 6, 6),
    catchup=False,
    tags=["hook_demo"],
) as dag:

    start_task = DummyOperator(task_id="start")
    bash_task = BashOperator(
        task_id="bash_command",
        bash_command="echo 'Hello from Bash Operator!'",
    )

    def transform_and_upload(s3_config: dict, ti=None):
        filename = s3_config["data_file_key"]
        bucket_name = s3_config["bucket_name"]
        key = s3_config["transformed_data_key"]
        try:
            with open(filename, "r") as file:
                lines = file.readlines()
            transformed_lines = [line.upper() for line in lines]
            hook = S3Hook(aws_conn_id="your_aws_connection")
            transformed_data = "\n".join(transformed_lines)
            hook.load_string(transformed_data, key, bucket_name, replace=True)
            print(f"Transformed data uploaded to s3://{bucket_name}/{key}")
        except Exception as e:
            print(f"Error transforming or uploading data: {e}")
            raise
        ti.xcom_push(key="transformed_lines", value=transformed_lines)
        return transformed_lines

    transform_task = PythonOperator(
        task_id="transform_and_upload",
        python_callable=transform_and_upload,
        op_kwargs={"s3_config": load_s3_config()},
    )

    def download_and_log(s3_config: dict, ti=None):
        hook = S3Hook(aws_conn_id="airflow-s3hook-demo")
        transformed_lines = ti.xcom_pull(task_ids="transform_and_upload")
        file_content = hook.read_key(
            key=s3_config["transformed_data_key"], bucket_name=s3_config["bucket_name"]
        )
        print("Downloaded file contents:")
        print(file_content)
        print("Transformed Lines:", transformed_lines)

    download_task = PythonOperator(
        task_id="download_and_log",
        python_callable=download_and_log,
        op_kwargs={"s3_config": load_s3_config()},
    )

    # Task Dependencies
    start_task >> bash_task >> transform_task >> download_task
