from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("ReadHDFSReddit")
        .getOrCreate()
    )

    df_hdfs = spark.read.csv(
        path="hdfs://namenode:8020/teste/reddit",
        header=True
    )

    df_hdfs.show(10)  # Apenas exibe as 10 primeiras linhas

    spark.stop()
