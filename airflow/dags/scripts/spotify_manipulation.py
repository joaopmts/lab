import argparse
from pyspark.sql.functions import lit, concat_ws, col
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession
from spotify_utils import Manipulation
import argparse


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input_path")
    p.add_argument("--input_format")
    p.add_argument("--temp")
    p.add_argument("--output_format")
    return p.parse_args()

def manipulatedf(df):

    df = df.drop_duplicates(subset=["track_id"])

    df = (
    df
    .withColumnRenamed("track_id", "id")
    .withColumnRenamed("track_name", "name")
    .withColumn("artists_song", concat_ws(" - ", "artists", "name"))
)   
    

    cols = [
    "valence","acousticness", "artists", "danceability", "duration_ms",
    "energy", "explicit", "id", "instrumentalness", "key", "liveness", "loudness",
    "mode", "name", "popularity", "speechiness", "tempo", "artists_song"
]
    numeric_str_cols = [
    "valence", "acousticness", "danceability", "duration_ms",
    "energy", "liveness", "loudness", "speechiness", "instrumentalness", "key",
    "tempo", "mode", "popularity"
]
    
    for c in numeric_str_cols:
        df = df.withColumn(c, col(c).cast(DoubleType()))

    df = df.select(cols)

    return df

def main():

    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("manipulation")
        .getOrCreate()
    )

    df = Manipulation.read_by_ext(spark, args.input_format, args.input_path)

    df = manipulatedf(df)

    Manipulation.write_by_ext(df, args.output_format, args.temp)

if __name__ == "__main__":
    main()