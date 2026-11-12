"""Utilitários compartilhados pelos jobs Spark do lab.

Importável de dois jeitos:
  * dentro do Airflow (DAGs/PythonOperator/plugins):  ``from scripts.lab_utils import ...``
  * em jobs enviados via spark-submit (o diretório do script entra no sys.path):
    ``from lab_utils import ...``

Por isso este módulo não importa nada do Airflow.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

SUPPORTED_FORMATS = ("json", "csv", "parquet")

# ---------------------------------------------------------------------------
# Reddit
# ---------------------------------------------------------------------------
REDDIT_POST_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("author", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("subreddit_id", StringType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("score", IntegerType(), True),
    StructField("upvote_ratio", DoubleType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("over_18", BooleanType(), True),
    StructField("stickied", BooleanType(), True),
    StructField("spoiler", BooleanType(), True),
    StructField("locked", BooleanType(), True),
    StructField("permalink", StringType(), True),
    StructField("url", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("link_flair_text", StringType(), True),
])


# Bronze = posts + timestamp de processamento. Use este schema ao LER o bronze: o JSON
# omite campos nulos, então inferir o schema pode "perder" colunas (ex.: selftext).
REDDIT_BRONZE_SCHEMA = StructType(
    REDDIT_POST_SCHEMA.fields + [StructField("timestamp", StringType(), True)]
)


def submission_to_dict(post) -> dict:
    """Converte um ``praw.models.Submission`` em dict com exatamente os campos do schema."""
    row = {f.name: getattr(post, f.name, None) for f in REDDIT_POST_SCHEMA.fields}
    # author e subreddit são objetos no praw; author é None para contas excluídas
    row["author"] = getattr(post.author, "name", None) if post.author else None
    row["subreddit"] = getattr(post.subreddit, "display_name", None)
    return row


def dedup_latest(df: DataFrame, key: str = "id", order_col: str = "timestamp") -> DataFrame:
    """Mantém uma linha por ``key`` — a mais recente segundo ``order_col``."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---------------------------------------------------------------------------
# Leitura / escrita genéricas
# ---------------------------------------------------------------------------
def _check_format(file_format: str) -> str:
    fmt = (file_format or "parquet").lower()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"Formato não suportado: {file_format!r}. Use um de {SUPPORTED_FORMATS}.")
    return fmt


def read_by_ext(spark: SparkSession, file_format: str, path: str,
                recursive: bool = True, csv_sep: str = ",",
                schema: StructType | None = None) -> DataFrame:
    fmt = _check_format(file_format)
    reader = spark.read.option("recursiveFileLookup", str(recursive).lower())
    if schema is not None:
        reader = reader.schema(schema)
    if fmt == "csv":
        return reader.option("header", True).option("sep", csv_sep).csv(path)
    if fmt == "json":
        return reader.json(path)
    return reader.parquet(path)


def write_by_ext(df: DataFrame, file_format: str, path: str,
                 mode: str = "overwrite", csv_sep: str = ";") -> None:
    fmt = _check_format(file_format)
    writer = df.write.mode(mode)
    if fmt == "csv":
        writer.option("header", True).option("sep", csv_sep).csv(path)
    elif fmt == "json":
        # Mantém campos nulos: sem isso colunas inteiras somem do arquivo
        writer.option("ignoreNullFields", False).json(path)
    else:
        writer.parquet(path)
