# dags/reddit_bronze_s3a_spark.py
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from operators.reddit_operator import RedditOperator
from hook.spark_hook import sparkhook
from hook.spotify_hook import SpotifyHook

input_path = f"s3a://bronze/recomendacao/kaggle-spotify"
output_path = f"s3a://silver/recomendacao/kaggle_spotify"
output_format = "parquet"
input_format = "csv"
temp = f"s3a://temp/spotify"

with DAG(
    dag_id="kaggle_spotify_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kaggle", "spotify", "", "enrichment"],
) as dag:

    spark = sparkhook()
    confhook, dcphook = spark.get_confdcp()

    s = SpotifyHook()
    client_id, client_secret, host = s.get_spotify_auth()

    task1 = SparkSubmitOperator(
        task_id="bronze_spotify_manipulation",
        application="/opt/airflow/dags/scripts/spotify_manipulation.py", 
        name="spotify_manipulation",
        conn_id="spark_default", 
        application_args=[
            "--input_path", input_path,
            "--input_format", input_format,
            "--output_format", output_format,
            "--temp", temp

        ],
        conf=confhook,
        jars=dcphook,
        driver_class_path=dcphook,
        dag=dag
    )

    task2 = SparkSubmitOperator(
        task_id="silver_spotify_enrichment",
        application="/opt/airflow/dags/scripts/spotify_enrichment.py", 
        name="spotify_enrichment",
        conn_id="spark_default", 
        application_args=[
            "--input_path", input_path,
            "--input_format", input_format,
            "--output_path", output_path,
            "--output_format", output_format,
            "--client_id", client_id,
            "--client_secret", client_secret,
            "--host", host,
            "--temp", temp

        ],
        conf=confhook,
        jars=dcphook,
        driver_class_path=dcphook,
        dag=dag
    )

    task1 >> task2