import json
import argparse
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)

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

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--output")
    p.add_argument("--file-format")
    p.add_argument("--temp_path")
    return p.parse_args()

def get_now():
    now = datetime.now()
    processing_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")
    return processing_timestamp, year, month, day, hour



def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("RedditBronze")
        .getOrCreate()
    )

    df = spark.read.option("recursiveFileLookup", "true").parquet(args.temp_path)

    now, year, month, day, hour = get_now()

    df = df.withColumn("timestamp", F.lit(now))

    final_path = f"{args.output}/{args.file_format}/{year}{month}{day}{hour}"

    if args.file_format == "json":
        df.write.mode("append").json(final_path)
    elif args.file_format == "csv":
        df.write.mode("append").csv(final_path, header=True)
    else:
        df.write.mode("append").parquet(final_path)

    spark.stop()

if __name__ == "__main__":
    main()
