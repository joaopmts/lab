from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.s3acon_pyopscript_test import meu_job  

with DAG(
    "s3a_consult_pyophook",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run = PythonOperator(
        task_id="spark_task",
        python_callable=meu_job,  # passa a função aqui
    )
