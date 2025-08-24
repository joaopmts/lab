from datetime import datetime
from pyspark.sql import SparkSession
import argparse

def get_reddit_post(df):
    return df.filter(df["author"].isNotNull())

def get_users_df(df):
    return df.select("id", "author", "created_utc")

def get_content(df):
    return df.select("id", "title", "selftext")

def date_parts():
    now = datetime.now()
    return now.strftime("%Y"), now.strftime("%m"), now.strftime("%d"), now.strftime("%H")

def export_parquet(df, to, subfolder):
    year, month, day, hour = date_parts()
    output_path = f"{to}/{year}/{month}/{day}/{hour}/{subfolder}"
    df.write.mode("overwrite").parquet(output_path)

def reddit_transformation(spark, src, to):
    df = spark.read.csv(path=src, header=True)
    posts_df = get_reddit_post(df)
    users_df = get_users_df(posts_df)
    content = get_content(posts_df)
    export_parquet(users_df, to, "users")
    export_parquet(content, to, "content")

if __name__ == "__main__":
    spark = SparkSession.builder.master("spark://spark-master:7077").getOrCreate()

    parser = argparse.ArgumentParser(description="Reddit Post Transformation")
    parser.add_argument("--src", required=True)
    parser.add_argument("--to", required=True)
    args = parser.parse_args()
    reddit_transformation(spark, args.src, args.to)

    