# dags/reddit_bronze_s3a_spark.py
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from operators.reddit_operator import RedditOperator
from hook.spark_hook import sparkhook
from hook.reddit_hook import RedditHook

tf = "day"
output_path = f"s3a://bronze/reddit/{tf}"
file_format = "json"
temp_path = "s3a://temp/reddit"
r_query = "bf6"
r_tf = "day"
sub_reddit = "all"
sort_reddit = "new"

with DAG(
    dag_id="reddit_bronze_s3a_spark_sso",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["reddit", "bronze", "s3a", "spark"],
) as dag:

    spark = sparkhook()
    confhook, dcphook = spark.get_confdcp()

    r = RedditHook()
    cli_id, cli_secret, userag = r.get_cli()

    task1 = SparkSubmitOperator(
        task_id="reddit_search",
        application="/opt/airflow/dags/scripts/reddit_search.py", 
        name="redditsearch",
        conn_id="spark_default", 
        application_args=[
            "--cli_id", cli_id,
            "--cli_secret", cli_secret,
            "--userag", userag,
            "--temp_path", temp_path,
            "--r_query", r_query,
            "--sub_reddit", sub_reddit,
            "--sort_reddit", sort_reddit,
            "--r_tf", r_tf,
        ],
        conf=confhook,
        jars=dcphook,
        driver_class_path=dcphook,
        dag=dag
    )


    task2 = SparkSubmitOperator(
        task_id="reddit_bronze_spark_submit",
        application="/opt/airflow/dags/scripts/reddit_bronze_sso.py", 
        name="RedditBronze",
        conn_id="spark_default", 
        application_args=[
            "--output", output_path,
            "--file-format", file_format,
            "--temp_path", temp_path,
        ],
        conf=confhook,
        jars=dcphook,
        driver_class_path=dcphook,
        dag=dag
    )

    task1 >> task2 
