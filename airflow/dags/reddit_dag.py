from datetime import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.reddit_operator import RedditOperator

path = "/home/airflow/host_documents/reddit_project"

def csv(ti):
    data = ti.xcom_pull(task_ids="reddit_search")
    out_path = os.path.join(path, f"reddit_post_titles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    df = pd.DataFrame(data)
    df.to_csv(out_path, index=False, encoding="utf-8")

with DAG(
    dag_id="reddit_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",  
    catchup=False,
    tags=["reddit"],
) as dag:

    task1 = RedditOperator(
        task_id="reddit_search",
        query="bf6",       
    )

    task2 = PythonOperator(
        task_id="savecsv",
        python_callable=csv,
    )

    task1 >> task2
