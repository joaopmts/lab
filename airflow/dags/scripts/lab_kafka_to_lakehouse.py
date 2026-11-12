"""Job Spark do exemplo de integração (DAG lab_end_to_end).

Kafka (lab_events, produzido por NiFi e Airflow)
  -> Iceberg  iceberg.lab.events          (Hive Metastore + MinIO)
  -> Delta    spark_catalog.lab.events_summary
  -> Kafka    lab_events_summary          (resumo de volta para o broker)

Lê o tópico inteiro em batch e reescreve as tabelas: idempotente.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

BOOTSTRAP = "broker:19092"
SOURCE_TOPIC = "lab_events"
SUMMARY_TOPIC = "lab_events_summary"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("source", StringType()),
    StructField("event_type", StringType()),
    StructField("value", DoubleType()),
    StructField("ts", StringType()),
])


def main():
    spark = SparkSession.builder.appName("lab_kafka_to_lakehouse").getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.lab")  # mesmo database no HMS para Delta e Iceberg

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    events = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
                   F.col("timestamp").alias("kafka_ts"))
        .select("e.*", "kafka_ts")
        .filter(F.col("event_id").isNotNull())
        .dropDuplicates(["event_id"])
    )
    events.writeTo("iceberg.lab.events").createOrReplace()

    summary = (
        spark.table("iceberg.lab.events")
        .groupBy("source", "event_type")
        .agg(F.count("*").alias("n_events"), F.round(F.sum("value"), 2).alias("total_value"))
    )
    summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable("spark_catalog.lab.events_summary")

    (
        spark.table("spark_catalog.lab.events_summary")
        .select(F.concat_ws(":", "source", "event_type").alias("key"),
                F.to_json(F.struct("*")).alias("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", SUMMARY_TOPIC)
        .save()
    )

    n = spark.table("iceberg.lab.events").count()
    print(f"{n} eventos em iceberg.lab.events; resumo em spark_catalog.lab.events_summary e no tópico {SUMMARY_TOPIC}")
    spark.stop()


if __name__ == "__main__":
    main()
