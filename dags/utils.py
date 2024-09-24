import json
from datetime import timedelta
import os
from airflow.models import Variable


# Default args for DAG
def load_default_args():
    return {
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=5),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    }


def load_s3_config(s3_config_file: str = "s3_config.json") -> dict:
    config_path = "/opt/airflow/config"
    s3_config_file = os.path.join(config_path, s3_config_file)  # Adjusted path
    with open(s3_config_file, "r") as f:
        return json.load(f)
