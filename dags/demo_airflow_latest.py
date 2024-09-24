import logging

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils import load_default_args, load_s3_config


@dag(
    default_args=load_default_args(),
    description="A simple DAG demonstrating latest Airflow features",
    schedule_interval=None,
    start_date=datetime(2024, 6, 6),
    catchup=False,
    tags=["demo"],
)
def latest_airflow_dag():
    @task
    def start_task():
        logging.info("Starting the DAG")
        return "Start"

    @task
    def process_data(start_message):
        logging.info(f"Processing data with message: {start_message}")
        processed_data = f"{start_message} - Data processed"
        return processed_data

    @task
    def upload_to_s3(processed_data):
        logging.info(f"Uploading the following data to S3: {processed_data}")

        # Using S3Hook
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_hook.load_string(processed_data, "test_bucket", "test_file.txt")

    @task
    def write_to_database(processed_data):
        logging.info(f"Writing the following data to Postgres: {processed_data}")

        # Using PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        postgres_hook.run(f"INSERT INTO test_table (data) VALUES ('{processed_data}')")

    @task
    def custom_operator_task(processed_data):
        logging.info(f"Custom Operator Task with data: {processed_data}")

    start = start_task()
    processed_data = process_data(start)
    upload_to_s3(processed_data)
    write_to_database(processed_data)
    custom_operator_task(processed_data)


latest_airflow_dag_instance = latest_airflow_dag()
