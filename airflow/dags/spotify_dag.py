"""Dataset Spotify (Kaggle) -> tratamento -> enriquecimento com ano via API -> silver.

Manual (schedule=None): a entrada é um dataset estático e o enriquecimento chama a API
do Spotify para cada faixa — rodar de hora em hora só repetia o mesmo trabalho.
Credenciais vão para o driver por ``env_vars`` (Jinja), não por argumentos.
"""
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

SCRIPTS = "/opt/airflow/dags/scripts"
LAB_UTILS = f"{SCRIPTS}/lab_utils.py"

INPUT_PATH = "s3a://bronze/recomendacao/kaggle-spotify"
INPUT_FORMAT = "csv"
OUTPUT_PATH = "s3a://silver/recomendacao/kaggle_spotify"
OUTPUT_FORMAT = "parquet"
TEMP = "s3a://temp/spotify"

with DAG(
    dag_id="spotify_api_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spotify", "silver", "s3a", "spark"],
) as dag:

    df_manipulation = SparkSubmitOperator(
        task_id="df_manipulation",
        application=f"{SCRIPTS}/spotify_manipulation.py",
        py_files=LAB_UTILS,
        name="manipulation",
        conn_id="spark_default",
        application_args=[
            "--input_path", INPUT_PATH,
            "--input_format", INPUT_FORMAT,
            "--temp", TEMP,
        ],
    )

    df_enrichment = SparkSubmitOperator(
        task_id="df_enrichment",
        application=f"{SCRIPTS}/spotify_enrichment.py",
        py_files=LAB_UTILS,
        name="enrichment",
        conn_id="spark_default",
        env_vars={
            "SPOTIFY_CLIENT_ID": "{{ conn.spotify_default.login }}",
            "SPOTIFY_CLIENT_SECRET": "{{ conn.spotify_default.password }}",
        },
        application_args=[
            "--temp", TEMP,
            "--output_path", OUTPUT_PATH,
            "--output_format", OUTPUT_FORMAT,
        ],
    )

    df_manipulation >> df_enrichment
