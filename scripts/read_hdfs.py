from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ReadHDFS").getOrCreate()

    df_hdfs = spark.read.csv(
        path='hdfs://namenode:8020/bronze/reddit',
        header=True
    )

    df_hdfs.show()

    spark.stop()