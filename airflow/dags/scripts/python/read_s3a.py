import requests
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import os

if __name__ == "__main__":

    spark = SparkSession \
    .builder \
    .master("spark://spark-master:7077").getOrCreate()

    df_hdfs = spark.read.csv(
        path="s3a://bronze/reddit",
        header=True
    )

    df_hdfs.show(10)
    spark.stop()
