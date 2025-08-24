from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from hook.s3a_hook import S3AHook

with DAG(
    dag_id="s3a_consult_hook_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None, 
    catchup=False,
    tags=["spark", "s3a", "reddit"],
) as dag:

    hook = S3AHook()
    confhook, dcphook = hook.get_conn()

    spark_job_currency = SparkSubmitOperator(
    task_id="s3a_consult_hook",
    application="/opt/airflow/dags/scripts/python/read_s3a.py",
    name="s3a_consult",
    conn_id="spark_default",
    conf=confhook,
    jars=dcphook,
    driver_class_path=dcphook,
    dag=dag)


    spark_job_currency