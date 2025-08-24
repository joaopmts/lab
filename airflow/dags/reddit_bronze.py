from datetime import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.reddit_operator import RedditOperator
from scripts.reddit_bronze import write  

tf = "day"
path = f"s3a://bronze/reddit/{tf}"
file_format = "json"

with DAG(
    dag_id="reddit_bronze_s3a",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",  
    catchup=False,
    tags=["reddit","bronze","s3a"],
) as dag:

    task1 = RedditOperator(
        task_id="reddit_search",
        query="bf6",      
        time_filter=tf 
    )

    task2 = PythonOperator(
        task_id="reddit_bronze",
        python_callable=write,
        op_kwargs={
            "path": path,               
            "file_format": file_format,
            "source_task_id": "reddit_search",
        },
    )


    task1 >> task2