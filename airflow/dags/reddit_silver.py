"""bronze -> silver do Reddit usando PythonOperator com Spark no worker.

Disparado pelo Asset de ``reddit_bronze_s3a``.
"""
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset

from scripts.reddit_silver import write

TIME_FILTER = "day"
SRC_FORMAT = "json"
SRC = f"s3a://bronze/reddit_pyop/{TIME_FILTER}/{SRC_FORMAT}"
DEST = "s3a://silver/reddit_pyop"
FILE_FORMAT = "json"

REDDIT_BRONZE_PYOP = Asset(uri=SRC, name="reddit_bronze_pyop")

with DAG(
    dag_id="reddit_silver_s3a",
    start_date=datetime(2025, 1, 1),
    schedule=[REDDIT_BRONZE_PYOP],
    catchup=False,
    max_active_runs=1,
    tags=["reddit", "silver", "s3a"],
) as dag:

    PythonOperator(
        task_id="reddit_silver",
        python_callable=write,
        op_kwargs={
            "src": SRC,
            "src_format": SRC_FORMAT,
            "destiny": DEST,
            "file_format": FILE_FORMAT,
        },
    )
