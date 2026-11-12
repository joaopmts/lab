"""Job Spark: enriquece as faixas com o ano de lançamento via API do Spotify.

Credenciais em SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET (definidas pelo DAG via env_vars).
"""
import argparse
import os
import time

import spotipy
from pyspark.sql import SparkSession
from spotipy.oauth2 import SpotifyClientCredentials

from lab_utils import write_by_ext

BATCH_SIZE = 50  # limite do endpoint /tracks


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--temp", required=True)
    p.add_argument("--output_path", required=True)
    p.add_argument("--output_format", default="parquet")
    return p.parse_args()


def spotify_conn():
    return spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=os.environ["SPOTIFY_CLIENT_ID"],
            client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
        )
    )


def get_year(sp, track_ids, years):
    for t in sp.tracks(track_ids)["tracks"]:
        if not t:
            continue
        release_date = (t.get("album") or {}).get("release_date") or ""
        year = int(release_date.split("-")[0]) if release_date[:4].isdigit() else None
        years.append((t["id"], year))


def enrich_with_year(df, sp, spark, sleep_time=0.1):
    all_results, batch = [], []
    n_rows = n_batches = 0

    def flush():
        nonlocal batch, n_batches
        get_year(sp, batch, all_results)
        n_batches += 1
        print(f"Batch {n_batches}: {len(batch)} músicas (linha {n_rows})")
        batch = []
        time.sleep(sleep_time)

    for row in df.select("id").toLocalIterator():
        n_rows += 1
        batch.append(row["id"])
        if len(batch) == BATCH_SIZE:
            flush()
    if batch:
        flush()

    df_years = spark.createDataFrame(all_results, "id string, year int")
    return df.join(df_years, on="id", how="left")


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("spotify_enrichment").getOrCreate()

    df = spark.read.parquet(args.temp)
    df = enrich_with_year(df, spotify_conn(), spark)
    write_by_ext(df, args.output_format, args.output_path, mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()
