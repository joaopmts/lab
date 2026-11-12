"""Job Spark de teste: lê CSVs de s3a://teste/reddit e mostra as 10 primeiras linhas."""
from pyspark.sql import SparkSession

PATH = "s3a://teste/reddit"

if __name__ == "__main__":
    # master vem do spark-submit (connection spark_default)
    spark = SparkSession.builder.appName("ReadS3ATest").getOrCreate()
    spark.read.csv(path=PATH, header=True).show(10)
    spark.stop()
