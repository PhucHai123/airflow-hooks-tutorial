from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os


class CustomS3Hook(S3Hook):
    def __init__(self, aws_conn_id="aws_default") -> None:
        super().__init__(aws_conn_id=aws_conn_id)

    def upload_files(self, bucket_name, local_folder) -> None:
        # Get a list of all files in the local directory
        files = [
            f
            for f in os.listdir(local_folder)
            if os.path.isfile(os.path.join(local_folder, f))
        ]
        for file in files:
            file_path = os.path.join(local_folder, file)
            # Use the pre-built load_file method to upload each file
            self.load_file(filename=file_path, key=file, bucket_name=bucket_name)
