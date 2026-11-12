"""Job Spark: busca posts no Reddit e grava em uma área temporária (parquet).

Credenciais chegam por variáveis de ambiente (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET,
REDDIT_USER_AGENT) — definidas pelo DAG via ``env_vars`` — e não por argumentos de
linha de comando, que apareceriam nos logs e no Spark UI.
"""
import argparse
import os

import praw
from pyspark.sql import SparkSession

from lab_utils import REDDIT_POST_SCHEMA, submission_to_dict


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--temp_path", required=True)
    p.add_argument("--r_query", required=True)
    p.add_argument("--sub_reddit", default="all")
    p.add_argument("--sort_reddit", default="new")
    p.add_argument("--r_tf", default="day")
    return p.parse_args()


def reddit_conn() -> praw.Reddit:
    return praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ["REDDIT_USER_AGENT"],
    )


def reddit_search(reddit, query, subreddit, sort, time_filter):
    results = reddit.subreddit(subreddit).search(query=query, sort=sort, time_filter=time_filter)
    return [submission_to_dict(p) for p in results]


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("RedditSearchforBronze").getOrCreate()

    data = reddit_search(
        reddit=reddit_conn(),
        query=args.r_query,
        subreddit=args.sub_reddit,
        sort=args.sort_reddit,
        time_filter=args.r_tf,
    )
    print(f"{len(data)} posts encontrados")

    df = spark.createDataFrame(data, schema=REDDIT_POST_SCHEMA)
    df.write.mode("overwrite").parquet(args.temp_path)

    spark.stop()


if __name__ == "__main__":
    main()
