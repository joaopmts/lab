"""Testes de leitura do MinIO (s3a://teste/reddit) pelo Spark, de duas formas:

* ``spark_submit``  -> SparkSubmitOperator (driver em processo spark-submit)
* ``python_session`` -> PythonOperator criando a SparkSession via SparkHook

Substitui os antigos s3a_consult_simple / s3a_consult_sparksubophook / s3a_consult_pyophook.
Nenhuma credencial no código: o S3A lê AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY do ambiente.
"""
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from scripts.s3acon_pyopscript_test import meu_job

with DAG(
    dag_id="s3a_consult_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "s3a", "test"],
) as dag:

    SparkSubmitOperator(
        task_id="spark_submit",
        application="/opt/airflow/dags/scripts/read_s3a_teste.py",
        name="s3a_consult",
        conn_id="spark_default",
        verbose=True,
    )

    PythonOperator(
        task_id="python_session",
        python_callable=meu_job,
    )
