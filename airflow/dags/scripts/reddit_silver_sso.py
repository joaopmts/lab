"""Job Spark: camada silver do Reddit a partir de todo o bronze.

Lê o bronze inteiro, descarta posts sem autor, deduplica por ``id`` (mantém a versão
mais recente) e reescreve ``users`` e ``content`` com ``overwrite``. Assim a silver é
idempotente: rodar N vezes gera o mesmo resultado, sem acumular duplicatas.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from lab_utils import REDDIT_BRONZE_SCHEMA, dedup_latest, read_by_ext, write_by_ext


def get_reddit_post(df):
    return df.filter(col("author").isNotNull())


def get_users_df(df):
    return df.select("id", "author", "created_utc")


def get_content(df):
    return df.select("id", "title", "selftext")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--src_format", default="json")
    parser.add_argument("--destiny", required=True)
    parser.add_argument("--file_format", default="json")
    parser.add_argument("--appname", default="RedditSilver")
    args = parser.parse_args()

    spark = SparkSession.builder.appName(args.appname).getOrCreate()

    df = read_by_ext(spark, args.src_format, args.src, schema=REDDIT_BRONZE_SCHEMA)
    posts = dedup_latest(get_reddit_post(df))

    base = f"{args.destiny}/{args.file_format}"
    write_by_ext(get_users_df(posts), args.file_format, f"{base}/users", mode="overwrite")
    write_by_ext(get_content(posts), args.file_format, f"{base}/content", mode="overwrite")
    print(f"Silver gravada em {base}/users e {base}/content")

    spark.stop()


if __name__ == "__main__":
    main()
