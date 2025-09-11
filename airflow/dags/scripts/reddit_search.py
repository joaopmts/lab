import praw
import argparse
from pyspark.sql import SparkSession, Row
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


def reddit_conn(client_id, client_secret, user_agent):
    reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )
    return reddit

def reddit_search(reddit, query, subreddit, sort, time_filter):
        
    results = reddit.subreddit(subreddit).search(
            query=query,
            sort=sort,
            time_filter=time_filter,
        )
    
    rows = []
    for p in results:
        rows.append({
            "id": getattr(p, "id", None),
            "name": getattr(p, "name", None),
            "title": getattr(p, "title", None),
            "selftext": getattr(p, "selftext", None),
            "author": getattr(p.author, "name", None),
            "subreddit": getattr(p.subreddit, "display_name", None),
            "subreddit_id": getattr(p, "subreddit_id", None),
            "created_utc": getattr(p, "created_utc", None),
            "score": getattr(p, "score", None),
            "upvote_ratio": getattr(p, "upvote_ratio", None),
            "num_comments": getattr(p, "num_comments", None),
            "over_18": getattr(p, "over_18", None),
            "stickied": getattr(p, "stickied", None),
            "spoiler": getattr(p, "spoiler", None),
            "locked": getattr(p, "locked", None),
            "permalink": getattr(p, "permalink", None),
            "url": getattr(p, "url", None),
            "domain": getattr(p, "domain", None),
            "link_flair_text": getattr(p, "link_flair_text", None),
        })
    return rows

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--cli_id")
    p.add_argument("--cli_secret")
    p.add_argument("--userag")
    p.add_argument("--temp_path")
    p.add_argument("--r_query")
    p.add_argument("--sub_reddit")
    p.add_argument("--sort_reddit")
    p.add_argument("--r_tf")
    return p.parse_args()


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("RedditSearchforBronze").getOrCreate()

    reddit = reddit_conn(args.cli_id, args.cli_secret, args.userag)

    data = reddit_search(
        reddit=reddit,
        query=args.r_query,
        subreddit=args.sub_reddit,
        sort=args.sort_reddit,
        time_filter=args.r_tf,
    )

    df = spark.createDataFrame([Row(**r) for r in data], schema=REDDIT_POST_SCHEMA)

    df.write.mode("overwrite").parquet(args.temp_path)

    spark.stop()


if __name__ == "__main__":
    main()
