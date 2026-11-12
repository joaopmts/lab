"""Job Spark: move o resultado temporário da busca para a camada bronze.

Grava em ``{output}/{file_format}/{partition}`` com ``overwrite``: a partição vem do
intervalo do DAG run, então reexecutar a mesma run não duplica dados.
"""
import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

from lab_utils import write_by_ext


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True)
    p.add_argument("--file-format", dest="file_format", default="json")
    p.add_argument("--temp_path", required=True)
    p.add_argument("--partition", required=True, help="ex.: 2026111114 (YYYYMMDDHH)")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("RedditBronze").getOrCreate()

    processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df = (
        spark.read.parquet(args.temp_path)
        .withColumn("timestamp", F.lit(processed_at))
    )

    final_path = f"{args.output}/{args.file_format}/{args.partition}"
    write_by_ext(df, args.file_format, final_path, mode="overwrite")
    print(f"Bronze gravado em {final_path}")

    spark.stop()


if __name__ == "__main__":
    main()
