from airflow import Dataset
from airflow.decorators import dag
from datetime import datetime
from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator,
    S3CopyObjectOperator,
)

MY_S3_BUCKET = "pch-test-bucket"
MY_FOLDER = "s3tos3source/"
MY_FILENAME = "job-without-var.py"
MY_S3_BUCKET_DELIMITER = "/"
AWS_CONN_ID = "s3_connection"
MY_S3_BUCKET_TO_COPY_TO = "s3tos3target"

my_dataset = Dataset(
    f"s3://{MY_S3_BUCKET}{MY_S3_BUCKET_DELIMITER}{MY_FOLDER}{MY_FILENAME}"
)

@dag(
    dag_id="from-s3-to-s3",
    start_date=datetime(2023, 7, 17),
    schedule=[my_dataset],
    catchup=False,
)
def from_s3_to_s3():
    # list all existing files in MY_FOLDER in MY_S3_BUCKET
    list_files = S3ListOperator(
        task_id=f"list_files",
        aws_conn_id=AWS_CONN_ID,
        bucket=MY_S3_BUCKET,
        prefix=MY_FOLDER,
        delimiter=MY_S3_BUCKET_DELIMITER,
    )

    # copy all files to MY_S3_BUCKET_TO_COPY_TO
    copy_files = S3CopyObjectOperator.partial(
        task_id="copy_files",
        aws_conn_id=AWS_CONN_ID,
    ).expand_kwargs(
        list_files.output.map(
            lambda x: {
                "source_bucket_key": f"s3://{MY_S3_BUCKET}{MY_S3_BUCKET_DELIMITER}{x}",
                "dest_bucket_key": f"s3://{MY_S3_BUCKET_TO_COPY_TO}{MY_S3_BUCKET_DELIMITER}{x}",
            }
        )
    )

    list_files >> copy_files

from_s3_to_s3()
