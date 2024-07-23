from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from datetime import datetime, timedelta

# DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Airflow DAG 정의
with DAG(
    'move_s3_objects',
    default_args=default_args,
    description='Move objects within AWS S3',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # 이동할 S3 객체 정보
    source_bucket_name = 'pch-test-bucket'
    source_object_key = 's3tos3source/data/job-without-var.py'

    dest_bucket_name = 'pch-test-bucket'
    dest_object_key = 's3tos3target'

    # S3 객체 복사 작업 정의
    move_object = S3CopyObjectOperator(
        task_id='move_object',
        source_bucket_name=source_bucket_name,
        source_bucket_key=source_object_key,
        dest_bucket_name=dest_bucket_name,
        dest_bucket_key=dest_object_key,
        aws_conn_id='s3_connection',  # AWS 연결 ID 설정
        replace=True,  # 목적지에 같은 이름의 객체가 이미 있는 경우 덮어쓸지 여부
    )

    # DAG 실행 순서 정의
    move_object

# DAG 객체 반환
dag

