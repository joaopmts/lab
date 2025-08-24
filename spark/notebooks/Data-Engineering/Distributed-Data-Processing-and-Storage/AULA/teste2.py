import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName('spark-standalone')
    .master('spark://spark-master:7077')
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "eDFFCzvdNdoeLlJkZhXI")
    .config("spark.hadoop.fs.s3a.secret.key", "gU3PQYFs6K60DvP5ut52zMQJAU3M3rW4XtCY9lqb")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# Caminho correto no MinIO
path_bronze_reddit = "s3a://bronze/reddit"

# Leitura dos CSVs
df_bronze_reddit = spark.read.option("header", "true").csv(path_bronze_reddit)

# Mostrar algumas linhas
df_bronze_reddit.show(10, truncate=False)
df_bronze_reddit.printSchema()
