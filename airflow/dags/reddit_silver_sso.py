from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from hook.spark_hook import sparkhook


tf = "day"
file_format_in = "json"
src = f"s3a://bronze/reddit/{tf}/{file_format_in}"
dest = "s3a://silver/reddit"
file_format = "json"

with DAG(
    dag_id="reddit_silver_s3a_sso",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["reddit", "silver", "s3a"],
) as dag:

    spark = sparkhook()
    confhook, dcphook = spark.get_confdcp()

    reddit_silver = SparkSubmitOperator(
        task_id="reddit_silver",
        application="/opt/airflow/dags/scripts/reddit_silver_sso.py",
        name="redditsilver",
        conn_id="spark_default",
        application_args=[
            "--src", src,
            "--destiny", dest,
            "--file_format", file_format,
            "--appname", "RedditSilver",
        ],
        conf=confhook,
        jars=dcphook,
        driver_class_path=dcphook,
        dag=dag
    )

    reddit_silver
