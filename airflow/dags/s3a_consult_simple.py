from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

minio_conf = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "eDFFCzvdNdoeLlJkZhXI",
    "spark.hadoop.fs.s3a.secret.key": "gU3PQYFs6K60DvP5ut52zMQJAU3M3rW4XtCY9lqb",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}

jar_home="/opt/airflow/conf/jars"
driver_class_path=f'{jar_home}/hadoop-aws-3.3.4.jar,{jar_home}/aws-java-sdk-bundle-1.12.262.jar'

# Definição da DAG
with DAG(
    dag_id="s3a_consult_hard",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual, pode mudar para cron
    catchup=False,
    tags=["spark", "s3a", "reddit"],
) as dag:

    spark_job_currency = SparkSubmitOperator(
    task_id="s3a_consult",
    application="/opt/airflow/dags/scripts/read_s3a_teste.py",
    name="s3a",
    conn_id="spark_default",
    conf=minio_conf,
    jars=driver_class_path,
    driver_class_path=driver_class_path,
    dag=dag)


    spark_job_currency