from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from hook.spark_hook import sparkhook

with DAG(
    dag_id="s3a_consult_hook_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None, 
    catchup=False,
    tags=["spark", "s3a", "reddit"],
) as dag:

    spark = sparkhook()
    confhook, dcphook = spark.get_confdcp()

    spark_job_currency = SparkSubmitOperator(
    task_id="s3a_consult_hook",
    application="/opt/airflow/dags/scripts/read_s3a_teste.py",
    name="s3a_consult",
    conn_id="spark_default",
    conf=confhook,
    jars=dcphook,
    driver_class_path=dcphook,
    dag=dag)


    spark_job_currency