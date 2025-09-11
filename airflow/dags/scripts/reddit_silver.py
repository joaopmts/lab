from airflow.operators.python import get_current_context
from hook.spark_hook import sparkhook
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.functions import substring_index

def get_reddit_post(df):
    return df.filter(df["author"].isNotNull())

def get_users_df(df):
    return df.select("id", "author", "created_utc")

def get_content(df):
    return df.select("id", "title", "selftext")

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
        return salvar_json(df, final_path)
    elif file_format == "csv":
        return salvar_csv(df, final_path)
    else:
        return salvar_parquet(df, final_path)


def write(src, destiny, file_format, **kwargs):
    spark = sparkhook(appname="RedditSilver").get_conn()
    final_file = f"{destiny}/{file_format}"
    users_path = f"{final_file}/users/{year}{month}{day}{hour}"
    content_path = f"{final_file}/content/{year}{month}{day}{hour}"
    df = read_by_ext(spark, src)
    dftreated = get_reddit_post(df)
    users = get_users_df(dftreated)
    posts = get_content(dftreated)
    write_by_ext(users, file_format, users_path)
    write_by_ext(posts, file_format, content_path)
    
