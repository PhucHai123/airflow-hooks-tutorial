from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.slack.hooks.slack import SlackHook
from custom_hooks import CustomS3Hook
from utils import load_default_args, load_s3_config

# Load configurations
s3_config = load_s3_config()

# Define DAG default arguments
default_args = load_default_args()


# Define the custom operator tasks
@task
def upload_files_to_s3() -> None:
    s3_hook = CustomS3Hook(aws_conn_id="aws_default")
    s3_hook.upload_files(
        bucket_name=s3_config["bucket_name"], local_folder="/opt/airflow/data"
    )


@task
def notify_slack(success: bool) -> None:
    slack_hook = SlackHook(slack_conn_id="slack_default")
    if success:
        message = "All files have been successfully uploaded to S3."
    else:
        message = "File upload to S3 failed."

    slack_hook.call(
        api_method="chat.postMessage",
        json={"channel": "#test-airflow", "text": message},
    )


# Define the DAG
@dag(
    dag_id="file_upload_and_notify",
    default_args=default_args,
    start_date=datetime(2022, 5, 20),
    schedule_interval="@daily",
    catchup=False,
)
def file_upload_and_notify():
    upload_task = upload_files_to_s3()

    # Success and failure notifications
    notify_success = notify_slack(success=True)
    notify_failure = notify_slack(success=False)

    upload_task >> notify_success
    upload_task.on_failure_callback = notify_failure


# Instantiate the DAG
file_upload_and_notify()
