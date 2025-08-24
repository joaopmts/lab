from datetime import datetime
from pyspark.sql.functions import lit

def get_reddit_post(df):
    df_not_null = df.filter(df["author"].isNotNull()) #Filter chatbots
    return df_not_null

def get_users_df(df):
    users = df.select("id","author","created_utc")
    return users

def date():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def export_parquet(df, to):
    dt = date()
    df = df.withColumn("date", lit(dt))
    df.write.mode("overwrite").partitionBy("date").parquet(to)


def reddit_transformation(spark, src, to):
    dt = date()
    df = spark.read.csv(path=src, header=True)
    users_df = get_reddit_post(df)
    treated = get_users_df(users_df)
    export_parquet(treated, to)
