"""Callable de teste: SparkSession criada no worker do Airflow via SparkHook."""
from hook.spark_hook import SparkHook

PATH = "s3a://teste/reddit"


def meu_job():
    spark = SparkHook(appname="S3APythonOperatorTest").get_conn()
    try:
        spark.read.csv(path=PATH, header=True).show()
    finally:
        spark.stop()
