"""Callable do PythonOperator: grava no bronze os posts recebidos por XCom (Spark no worker do Airflow)."""
from datetime import datetime, timezone

from pyspark.sql import functions as F

from hook.spark_hook import SparkHook
from scripts.lab_utils import REDDIT_POST_SCHEMA, write_by_ext


def write(path, file_format, source_task_id, partition, ti, **_):
    data = ti.xcom_pull(task_ids=source_task_id) or []
    spark = SparkHook(appname="RedditBronze").get_conn()
    try:
        processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        posts = (
            spark.createDataFrame(data, schema=REDDIT_POST_SCHEMA)
            .withColumn("timestamp", F.lit(processed_at))
        )
        final_path = f"{path}/{file_format}/{partition}"
        write_by_ext(posts, file_format, final_path, mode="overwrite")
        print(f"{len(data)} posts gravados em {final_path}")
    finally:
        spark.stop()
