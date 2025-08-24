import os
from dotenv import load_dotenv
from s3fs import S3FileSystem

load_dotenv()

MINIO_SERVER_ACCESS_KEY = os.getenv("MINIO_SERVER_ACCESS_KEY")
MINIO_SERVER_SECRET_KEY = os.getenv("MINIO_SERVER_SECRET_KEY")
MINIO_URL = os.getenv("MINIO_URL")

def main():
    s3 = S3FileSystem(
        endpoint_url=MINIO_URL,
        key=MINIO_SERVER_ACCESS_KEY,
        secret=MINIO_SERVER_SECRET_KEY,
        use_ssl=False
    )

    buckets = s3.ls("")  # lista buckets
    print(buckets)

if __name__ == "__main__":
    main()
