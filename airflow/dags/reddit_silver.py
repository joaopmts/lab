from datetime import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.reddit_operator import RedditOperator
from scripts.reddit_silver import write  

tf = "day"
file_format_in = "json"
src = f"s3a://bronze/reddit/{tf}/{file_format_in}"
dest = "s3a://silver/reddit"
file_format = "json"

with DAG(
    dag_id="reddit_silver_s3a",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",  
    catchup=False,
    tags=["reddit","silver","s3a"],
) as dag:



    task2 = PythonOperator(
        task_id="reddit_silver",
        python_callable=write,
        op_kwargs={
            "src": src,               
            "destiny": dest,
           "file_format":file_format,
        },
    )


    task2