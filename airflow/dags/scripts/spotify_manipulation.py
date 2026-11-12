"""Job Spark: limpa o dataset do Spotify e grava em parquet na área temporária."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import DoubleType

from lab_utils import read_by_ext, write_by_ext

OUTPUT_COLS = [
    "valence", "acousticness", "artists", "danceability", "duration_ms",
    "energy", "explicit", "id", "instrumentalness", "key", "liveness", "loudness",
    "mode", "name", "popularity", "speechiness", "tempo", "artists_song",
]
NUMERIC_COLS = [
    "valence", "acousticness", "danceability", "duration_ms",
    "energy", "liveness", "loudness", "speechiness", "instrumentalness", "key",
    "tempo", "mode", "popularity",
]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input_path", required=True)
    p.add_argument("--input_format", default="csv")
    p.add_argument("--temp", required=True)
    return p.parse_args()


def manipulatedf(df):
    df = (
        df.dropDuplicates(subset=["track_id"])
        .withColumnRenamed("track_id", "id")
        .withColumnRenamed("track_name", "name")
        .withColumn("artists_song", concat_ws(" - ", "artists", "name"))
    )
    for c in NUMERIC_COLS:
        df = df.withColumn(c, col(c).cast(DoubleType()))
    return df.select(OUTPUT_COLS)


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("spotify_manipulation").getOrCreate()

    df = read_by_ext(spark, args.input_format, args.input_path)
    # A área temporária é sempre parquet: é o formato que spotify_enrichment.py lê
    write_by_ext(manipulatedf(df), "parquet", args.temp, mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()
