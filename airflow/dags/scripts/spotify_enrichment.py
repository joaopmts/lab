import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from pyspark.sql import SparkSession
from spotify_utils import Manipulation, Args
import time

def spotify_conn(client_id, client_secret):
    sp = spotipy.Spotify(
            auth_manager=SpotifyClientCredentials(
                client_id=client_id,
                client_secret=client_secret
            )
        )
    
    return sp

def get_year(sp, track_ids, years):
    results = sp.tracks(track_ids)["tracks"]
    for t in results:
        date = t["album"]["release_date"]
        year = int(date.split("-")[0])
        years.append((t["id"], year))


def enrich_with_year(df, sp, spark, sleep_time=0.1):

    ids_iter = df.select("id").toLocalIterator()

    all_results = []   
    batch = []         
    batch_size = 50   
    i = 0
    for row in ids_iter:
        i += 1
        batch.append(row["id"])

        if len(batch) == batch_size:

            get_year(sp, batch, all_results)
            print(f"Processado batch com {len(batch)} músicas, linha: {i}")

            batch = []
            time.sleep(sleep_time) 

    if batch:
        get_year(sp, batch, all_results)
        print(f"Processado último batch com {len(batch)} músicas")

    df_years = spark.createDataFrame(all_results, ["id", "year"])

    df_enriched = df.join(df_years, on="id", how="left")
    
    return df_enriched


def main():

    args = Args.parse_args()

    spark = (
        SparkSession.builder
        .appName("spotify_enrichment")
        .getOrCreate()
    )

    df = spark.read.parquet(args.temp)

    sp = spotify_conn(args.client_id, args.client_secret)

    df = enrich_with_year(df, sp, spark)

    Manipulation.write_by_ext(df, args.output_format, args.output_path)

if __name__ == "__main__":
    main()