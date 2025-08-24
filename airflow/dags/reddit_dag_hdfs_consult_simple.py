from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Definição da DAG
with DAG(
    dag_id="hdfs_consult",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual, pode mudar para cron
    catchup=False,
    tags=["spark", "hdfs", "reddit"],
) as dag:

    # Tarefa para executar o Spark job
    spark_job = SparkSubmitOperator(
        task_id="spark_hdfs_read",
        application="/opt/airflow/dags/scripts/python/read_hdfs.py", 
        conn_id="spark_default",  
        verbose=True,
    )

    spark_job
