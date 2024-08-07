import datetime
import logging
import os
import tempfile
from os import path

import pandas as pd
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from custom.hooks import MovielensHook
from custom.operators import GlueTriggerCrawlerOperator

from airflow import DAG

local_tz = pendulum.timezone("Asia/Seoul")


with DAG(
    dag_id="01_aws_usecase",
    description="DAG demonstrating some AWS-specific hooks and operators.",
    start_date=datetime.datetime(2023, 4, 10, tzinfo=local_tz),
    schedule_interval="@monthly",
    default_args={"depends_on_past": True},
) as dag:

    def _fetch_ratings(api_conn_id, s3_conn_id, s3_bucket, **context):
        now = datetime.datetime.now() 
        year = now.year
        month = now.month

        # Fetch ratings from our API.
        logging.info(f"Fetching ratings for {year}/{month:02d}")

        api_hook = MovielensHook(conn_id=api_conn_id)
        ratings = pd.DataFrame.from_records(
            api_hook.get_ratings_for_month(year=year, month=month),
            columns=["userId", "movieId", "rating", "timestamp"],
        )

        logging.info(f"Fetched {ratings.shape[0]} rows")

        # Write ratings to temp file.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, "ratings.csv")
            ratings.to_csv(tmp_path, index=False)

            # Upload file to S3.
            logging.info(f"Writing results to ratings/{year}/{month:02d}.csv")
            s3_hook = S3Hook(s3_conn_id)
            s3_hook.load_file(
                tmp_path,
                key=f"ratings/{year}/{month:02d}.csv",
                bucket_name=s3_bucket,
                replace=True,
            )

    fetch_ratings = PythonOperator(
        task_id="fetch_ratings",
        python_callable=_fetch_ratings,
        op_kwargs={
            "api_conn_id": "movielens",
            "s3_conn_id": "my_aws_conn",
            "s3_bucket": "pch-test-bucket",
        },
    )

    trigger_crawler = GlueTriggerCrawlerOperator(
        aws_conn_id="my_aws_conn",
        task_id="trigger_crawler",
        crawler_name="my-crawler",
        region_name="ap-northeast-2"
    )

    rank_movies = AthenaOperator(
        task_id="rank_movies",
        aws_conn_id="my_aws_conn",
        database="airflow",
        region_name="ap-northeast-2",
        query="""
            SELECT movieid, AVG(rating) as avg_rating, COUNT(*) as num_ratings
            FROM (
                SELECT movieid, rating, CAST(from_unixtime(timestamp) AS DATE) AS date
                FROM ratings
            )
            WHERE date <= DATE('{{ ds }}') 
            GROUP BY movieid
            ORDER BY avg_rating DESC
        """,
        output_location=f"s3://pch-test-bucket/airflow-output/{{{{ds}}}}",
    )

    fetch_ratings >> trigger_crawler >> rank_movies
