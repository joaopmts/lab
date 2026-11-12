"""Reddit -> bronze usando RedditOperator (XCom) + PythonOperator com Spark no worker.

Variante didática do pipeline SSO. Usa caminhos próprios (``reddit_pyop``) para não
sobrescrever os dados de ``reddit_bronze_s3a_spark_sso``.
"""
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset

from operators.reddit_operator import RedditOperator
from scripts.reddit_bronze import write

TIME_FILTER = "day"
PATH = f"s3a://bronze/reddit_pyop/{TIME_FILTER}"
FILE_FORMAT = "json"
PARTITION = "{{ (dag_run.data_interval_start or dag_run.run_after).strftime('%Y%m%d%H') }}"

REDDIT_BRONZE_PYOP = Asset(uri=f"{PATH}/{FILE_FORMAT}", name="reddit_bronze_pyop")

with DAG(
    dag_id="reddit_bronze_s3a",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["reddit", "bronze", "s3a"],
) as dag:

    reddit_search = RedditOperator(
        task_id="reddit_search",
        query="bf6",
        time_filter=TIME_FILTER,
    )

    reddit_bronze = PythonOperator(
        task_id="reddit_bronze",
        python_callable=write,
        op_kwargs={
            "path": PATH,
            "file_format": FILE_FORMAT,
            "source_task_id": "reddit_search",
            "partition": PARTITION,
        },
        outlets=[REDDIT_BRONZE_PYOP],
    )

    reddit_search >> reddit_bronze
