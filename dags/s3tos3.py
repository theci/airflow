from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'move_s3_objects',
    default_args=default_args,
    description='Move objects within AWS S3',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    source_bucket_name = 'pch-test-bucket'
    source_object_key = 's3tos3source/data/job-without-var.py'

    dest_bucket_name = 'pch-test-bucket'
    dest_object_key = 's3tos3target'

    # S3Hook을 사용하여 객체 복사
    def copy_s3_object(source_bucket_name, source_object_key, dest_bucket_name, dest_object_key, replace=True):
        s3_hook = S3Hook(aws_conn_id='s3_connection')
        s3_hook.copy_object(source_bucket_name, source_object_key, dest_bucket_name, dest_object_key, replace=replace)

    copy_object_task = PythonOperator(
        task_id='copy_object_task',
        python_callable=copy_s3_object,
        op_kwargs={
            'source_bucket_name': source_bucket_name,
            'source_object_key': source_object_key,
            'dest_bucket_name': dest_bucket_name,
            'dest_object_key': dest_object_key,
            'replace': True  # Replace 옵션 설정
        }
    )

    copy_object_task

dag

