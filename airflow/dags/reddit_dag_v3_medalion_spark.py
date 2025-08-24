from datetime import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.reddit_operator import RedditOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from hook.s3a_hook import S3AHook


with DAG(
    dag_id="reddit_medalion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["reddit", "medalion"],
) as dag:

    hook = S3AHook()
    confhook, dcphook = hook.get_conn()

    task2 = SparkSubmitOperator(
    task_id="Reddit_Post_Transformation",
    application="/opt/airflow/dags/scripts/spark/reddit_transformation_user.py",
    name="reddit_transformation",
    conn_id="spark_default",
    conf=confhook,
    jars=dcphook,
    driver_class_path=dcphook,
    application_args= [
            "--src", "hdfs://namenode:8020/bronze/reddit",
            "--to", "s3a://silver/reddit/airflow"],
    verbose=True,
    dag=dag
    )


    task2


