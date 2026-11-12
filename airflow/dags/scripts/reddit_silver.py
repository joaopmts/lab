"""Callable do PythonOperator: bronze -> silver do Reddit (Spark no worker do Airflow).

Mesma lógica de scripts/reddit_silver_sso.py: dedup por id + overwrite (idempotente).
"""
from pyspark.sql.functions import col

from hook.spark_hook import SparkHook
from scripts.lab_utils import REDDIT_BRONZE_SCHEMA, dedup_latest, read_by_ext, write_by_ext


def write(src, destiny, file_format, src_format="json", **_):
    spark = SparkHook(appname="RedditSilver").get_conn()
    try:
        df = read_by_ext(spark, src_format, src, schema=REDDIT_BRONZE_SCHEMA)
        posts = dedup_latest(df.filter(col("author").isNotNull()))

        base = f"{destiny}/{file_format}"
        write_by_ext(posts.select("id", "author", "created_utc"), file_format, f"{base}/users")
        write_by_ext(posts.select("id", "title", "selftext"), file_format, f"{base}/content")
        print(f"Silver gravada em {base}/users e {base}/content")
    finally:
        spark.stop()
