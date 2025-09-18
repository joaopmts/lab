# dags/reddit_bronze_s3a_spark.py
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from hook.spark_hook import sparkhook
from hook.spotify_hook import SpotifyHook

input_path = f"s3a://bronze/recomendacao/kaggle-spotify"
output_path = f"s3a://silver/recomendacao/kaggle_spotify"
output_format = "parquet"
input_format = "csv"
temp = f"s3a://temp/spotify"


with DAG(
    dag_id="spotify_api_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    spark = sparkhook()
    confhook, dcphook = spark.get_confdcp()

    s = SpotifyHook()
    client_id, client_secret = s.get_spotify_auth()

    task1 = SparkSubmitOperator(
        task_id="df_manipulation",
        application="/opt/airflow/dags/scripts/spotify_manipulation.py", 
        name="manipulation",
        conn_id="spark_default", 
        application_args=[
            "--output_format", output_format,
            "--temp", temp,
            "--input_path", input_path,
            "--input_format", input_format,
        ],
        conf=confhook,
        jars=dcphook,
        driver_class_path=dcphook,
        dag=dag
    )

    task2 = SparkSubmitOperator(
        task_id="df_enrichment",
        application="/opt/airflow/dags/scripts/spotify_enrichment.py", 
        name="enrichment",
        conn_id="spark_default", 
        application_args=[
            "--output_path", output_path,
            "--output_format", output_format,
            "--client_id", client_id,
            "--client_secret", client_secret,
            "--temp", temp,
        ],
        conf=confhook,
        jars=dcphook,
        driver_class_path=dcphook,
        dag=dag
    )

    task1 >> task2
