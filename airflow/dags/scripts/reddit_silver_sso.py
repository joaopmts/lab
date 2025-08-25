import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def get_reddit_post(df):
    return df.filter(col("author").isNotNull())

def get_users_df(df):
    return df.select("id", "author", "created_utc")

def get_content(df):
    return df.select("id", "title", "selftext")

def get_now():
    now = datetime.now()
    return {
        "ts": now.strftime("%Y-%m-%d %H:%M:%S"),
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
        "hour": now.strftime("%H"),
    }

def salvar_csv(df, final_file, sep=";", header=True, mode="append"):
    df.write.mode(mode).option("sep", sep).option("header", True).csv(final_file)

def salvar_json(df, final_file, mode="append"):
    df.write.mode(mode).json(final_file)

def salvar_parquet(df, final_file, mode="append"):
    df.write.mode(mode).parquet(final_file)

def ler_csv(spark, caminho_arquivo):
    return spark.read.option("recursiveFileLookup", "true").csv(path=caminho_arquivo, header=True)

def ler_json(spark, caminho_arquivo):
    return spark.read.option("recursiveFileLookup", "true").json(path=caminho_arquivo)

def ler_parquet(spark, caminho_arquivo):
    return spark.read.option("recursiveFileLookup", "true").parquet(path=caminho_arquivo)

def read_by_ext(spark, src):
    srcformat = src.rsplit("/", 1)[-1]
    if srcformat == "json":
        return ler_json(spark, src)
    if srcformat == "csv":
        return ler_csv(spark, src)
    else:
        return ler_parquet(spark, src)

def write_by_ext(df, file_format, final_path):
    if file_format == "json":
        salvar_json(df, final_path)
    if file_format == "csv":
        salvar_csv(df, final_path)
    else:
        salvar_parquet(df, final_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--destiny", required=True)
    parser.add_argument("--file_format")
    parser.add_argument("--appname")
    args = parser.parse_args()

    spark = SparkSession.builder.master("spark://spark-master:7077").getOrCreate()

    now = get_now()

    final_file = f"{args.destiny}/{args.file_format}"
    users_path = f"{final_file}/users/{now['year']}{now['month']}{now['day']}{now['hour']}"
    content_path = f"{final_file}/content/{now['year']}{now['month']}{now['day']}{now['hour']}"

    df = read_by_ext(spark, args.src)
    dftreated = get_reddit_post(df)
    users = get_users_df(dftreated)
    posts = get_content(dftreated)

    write_by_ext(users, args.file_format, users_path)
    write_by_ext(posts, args.file_format, content_path)

    spark.stop()

if __name__ == "__main__":
    main()
