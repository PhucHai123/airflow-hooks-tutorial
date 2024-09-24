# DAG exhibiting task flow paradigm in airflow 2.0
# https://airflow.apache.org/docs/apache-airflow/2.0.2/tutorial_taskflow_api.html
# Modified for our use case

import json
from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
}


@dag(default_args=default_args, schedule_interval="@daily", start_date=days_ago(2), tags=["example"])
def dag_demo():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """

    @task_group()
    def aws_activities():
        @task()
        def upload_to_s3(bucket_name, bucket_key, file_path):
            hook = S3Hook(aws_conn_id="aws_default")
            hook.load_file(file_path, bucket_key, bucket_name, replace=True)

        @task()
        def put_key_value_to_dynamo(messages: list):
            hook = DynamoDBHook(aws_conn_id="aws_default", table_name="homework")
            hook.write_batch_data(messages)

        import os

        relative_path = relative_path = os.path.dirname(__file__)
        file_path = os.path.join(relative_path, "hello.txt")
        upload_to_s3 = upload_to_s3("cdp-dev-regrep-airflow-datum", "homework/hello.txt", file_path)
        put_key_value_to_dynamo = put_key_value_to_dynamo([{"member_name": "Hung", "object": "demo"}])
        upload_to_s3 >> put_key_value_to_dynamo

    aws_activities()


dag_demo = dag_demo()
