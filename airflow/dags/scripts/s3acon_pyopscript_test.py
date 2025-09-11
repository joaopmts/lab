from hook.spark_hook import sparkhook

def meu_job():
    spark = sparkhook(appname="test").get_conn()

    df_hdfs = spark.read.csv(
        path="s3a://teste/reddit",
        header=True
    )

    df_hdfs.show()
    spark.stop()  # sempre bom encerrar a sessão

if __name__ == "__main__":
    meu_job()
