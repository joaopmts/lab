"""Reddit -> bronze (MinIO) com SparkSubmitOperator.

* Credenciais do Reddit são resolvidas em tempo de execução via Jinja (``conn.reddit_default``)
  e entregues ao driver por ``env_vars`` — nada é lido do banco no parse do DAG e nada
  aparece em ``application_args``.
* Config S3A/jars ficam na imagem (spark/conf/core-site.xml + $SPARK_HOME/jars).
* Ao terminar, publica o Asset ``REDDIT_BRONZE_SSO``, que dispara ``reddit_silver_s3a_sso``.
"""
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, Asset

SCRIPTS = "/opt/airflow/dags/scripts"
LAB_UTILS = f"{SCRIPTS}/lab_utils.py"

TIME_FILTER = "day"
OUTPUT_PATH = f"s3a://bronze/reddit/{TIME_FILTER}"
FILE_FORMAT = "json"
TEMP_PATH = "s3a://temp/reddit"
QUERY = "bf6"
SUBREDDIT = "all"
SORT = "new"

# Partição YYYYMMDDHH do intervalo do run (runs manuais não têm data_interval no Airflow 3)
PARTITION = "{{ (dag_run.data_interval_start or dag_run.run_after).strftime('%Y%m%d%H') }}"

REDDIT_BRONZE_SSO = Asset(uri=f"{OUTPUT_PATH}/{FILE_FORMAT}", name="reddit_bronze_sso")

with DAG(
    dag_id="reddit_bronze_s3a_spark_sso",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["reddit", "bronze", "s3a", "spark"],
) as dag:

    reddit_search = SparkSubmitOperator(
        task_id="reddit_search",
        application=f"{SCRIPTS}/reddit_search.py",
        py_files=LAB_UTILS,
        name="redditsearch",
        conn_id="spark_default",
        env_vars={
            "REDDIT_CLIENT_ID": "{{ conn.reddit_default.extra_dejson.client_id }}",
            "REDDIT_CLIENT_SECRET": "{{ conn.reddit_default.extra_dejson.client_secret }}",
            "REDDIT_USER_AGENT": "{{ conn.reddit_default.extra_dejson.user_agent }}",
        },
        application_args=[
            "--temp_path", TEMP_PATH,
            "--r_query", QUERY,
            "--sub_reddit", SUBREDDIT,
            "--sort_reddit", SORT,
            "--r_tf", TIME_FILTER,
        ],
    )

    reddit_bronze = SparkSubmitOperator(
        task_id="reddit_bronze_spark_submit",
        application=f"{SCRIPTS}/reddit_bronze_sso.py",
        py_files=LAB_UTILS,
        name="RedditBronze",
        conn_id="spark_default",
        application_args=[
            "--output", OUTPUT_PATH,
            "--file-format", FILE_FORMAT,
            "--temp_path", TEMP_PATH,
            "--partition", PARTITION,
        ],
        outlets=[REDDIT_BRONZE_SSO],
    )

    reddit_search >> reddit_bronze
