import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName('spark-standalone')
    .master('spark://spark-master:7077')
    .getOrCreate()
)

# Caminho correto no MinIO
path_bronze_reddit = "s3a://bronze/reddit"

# Leitura dos CSVs
df_bronze_reddit = spark.read.option("header", "true").csv(path_bronze_reddit)

# Mostrar algumas linhas
df_bronze_reddit.show(10, truncate=False)
df_bronze_reddit.printSchema()
