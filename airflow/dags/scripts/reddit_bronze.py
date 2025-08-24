from airflow.operators.python import get_current_context
from hook.spark_hook import sparkhook
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)
from pyspark.sql import functions as F
from datetime import datetime

REDDIT_POST_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("author", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("subreddit_id", StringType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("score", IntegerType(), True),
    StructField("upvote_ratio", DoubleType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("over_18", BooleanType(), True),
    StructField("stickied", BooleanType(), True),
    StructField("spoiler", BooleanType(), True),
    StructField("locked", BooleanType(), True),
    StructField("permalink", StringType(), True),
    StructField("url", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("link_flair_text", StringType(), True),
])

def get_now():
    now = datetime.now()
    processing_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")
    return processing_timestamp, year, month, day, hour

ts, year, month, day, hour = get_now()

def recievedata():
    ctx = get_current_context()
    return ctx

def to_df(data, spark):
    posts = spark.createDataFrame(data, schema=REDDIT_POST_SCHEMA)
    posts = posts.withColumn("timestamp", F.lit(ts))
    return posts

def write(path, file_format, source_task_id, **kwargs):
    spark = sparkhook(appname="RedditBronze").get_conn()
    ctx = recievedata()
    data = ctx["ti"].xcom_pull(task_ids=source_task_id)
    post = to_df(data, spark)
    final_file = f"{path}/{file_format}/{year}{month}{day}{hour}"
    if file_format == "json":
        post.write.mode("append").json(final_file)
    elif file_format == "csv":
        post.write.mode("append").csv(final_file, header=True)
    else:
        post.write.mode("append").parquet(final_file)
    spark.stop()