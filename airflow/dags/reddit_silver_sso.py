"""bronze -> silver do Reddit com SparkSubmitOperator.

Agendado pelo Asset publicado por ``reddit_bronze_s3a_spark_sso``: só roda depois que
o bronze terminou. O job é idempotente (dedup + overwrite), ver scripts/reddit_silver_sso.py.
"""
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, Asset

SCRIPTS = "/opt/airflow/dags/scripts"

TIME_FILTER = "day"
SRC_FORMAT = "json"
SRC = f"s3a://bronze/reddit/{TIME_FILTER}/{SRC_FORMAT}"
DEST = "s3a://silver/reddit"
FILE_FORMAT = "json"

REDDIT_BRONZE_SSO = Asset(uri=SRC, name="reddit_bronze_sso")

with DAG(
    dag_id="reddit_silver_s3a_sso",
    start_date=datetime(2025, 1, 1),
    schedule=[REDDIT_BRONZE_SSO],
    catchup=False,
    max_active_runs=1,
    tags=["reddit", "silver", "s3a", "spark"],
) as dag:

    SparkSubmitOperator(
        task_id="reddit_silver",
        application=f"{SCRIPTS}/reddit_silver_sso.py",
        py_files=f"{SCRIPTS}/lab_utils.py",
        name="redditsilver",
        conn_id="spark_default",
        application_args=[
            "--src", SRC,
            "--src_format", SRC_FORMAT,
            "--destiny", DEST,
            "--file_format", FILE_FORMAT,
            "--appname", "RedditSilver",
        ],
    )
