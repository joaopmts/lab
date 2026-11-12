"""Reddit -> um CSV local por execução em /opt/airflow/data/reddit_project."""
import os
from datetime import datetime

import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from operators.reddit_operator import RedditOperator

OUTPUT_DIR = "/opt/airflow/data/reddit_project"


def save_csv(ti, ts_nodash, **_):
    data = ti.xcom_pull(task_ids="reddit_search") or []
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"reddit_post_titles_{ts_nodash}.csv")
    pd.DataFrame(data).to_csv(out_path, index=False, encoding="utf-8")
    print(f"{len(data)} posts salvos em {out_path}")


with DAG(
    dag_id="reddit_one_file",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["reddit", "one file"],
) as dag:

    reddit_search = RedditOperator(
        task_id="reddit_search",
        query="bf6",
        time_filter="day",
    )

    savecsv = PythonOperator(
        task_id="savecsv",
        python_callable=save_csv,
    )

    reddit_search >> savecsv
