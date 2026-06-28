#!/usr/bin/env python3
"""
etl_job.py
──────────
PySpark ETL: Reads all 5 IMDb TSV files → cleans & joins → writes
Snappy-compressed Parquet files partitioned for efficient OLAP queries.

Source files (tab-separated, \\N = null):
  title.basics.tsv     — title metadata (type, year, genre, runtime)
  title.ratings.tsv    — weighted avg rating + vote count
  name.basics.tsv      — people (actors, directors, writers …)
  title.principals.tsv — cast/crew per title (links titles → people)
  title.akas.tsv       — alternate titles by region/language

Output datasets → data/lake/:
  titles/      partitionBy("titleType", "decade")
  people/      partitionBy("primaryProfession0")   ← first profession
  principals/  partitionBy("category")
  akas/        partitionBy("region")

Usage (local test):
  python3 etl_job.py --input ./data/raw --output ./data/lake

Usage (via Docker / make etl):
  spark-submit /opt/bitnami/spark/jobs/etl_job.py \\
      --input /data/raw --output /data/lake
"""

import argparse
import logging
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Schema definitions ────────────────────────────────────────────────────────
# All columns start as StringType — we cast after the null replacement.

BASICS_SCHEMA = StructType([
    StructField("tconst",         StringType(), True),
    StructField("titleType",      StringType(), True),
    StructField("primaryTitle",   StringType(), True),
    StructField("originalTitle",  StringType(), True),
    StructField("isAdult",        StringType(), True),
    StructField("startYear",      StringType(), True),
    StructField("endYear",        StringType(), True),
    StructField("runtimeMinutes", StringType(), True),
    StructField("genres",         StringType(), True),
])

RATINGS_SCHEMA = StructType([
    StructField("tconst",        StringType(), True),
    StructField("averageRating", StringType(), True),
    StructField("numVotes",      StringType(), True),
])

NAME_BASICS_SCHEMA = StructType([
    StructField("nconst",             StringType(), True),
    StructField("primaryName",        StringType(), True),
    StructField("birthYear",          StringType(), True),
    StructField("deathYear",          StringType(), True),
    StructField("primaryProfession",  StringType(), True),
    StructField("knownForTitles",     StringType(), True),
])

PRINCIPALS_SCHEMA = StructType([
    StructField("tconst",     StringType(), True),
    StructField("ordering",   StringType(), True),
    StructField("nconst",     StringType(), True),
    StructField("category",   StringType(), True),
    StructField("job",        StringType(), True),
    StructField("characters", StringType(), True),
])

AKAS_SCHEMA = StructType([
    StructField("titleId",         StringType(), True),
    StructField("ordering",        StringType(), True),
    StructField("title",           StringType(), True),
    StructField("region",          StringType(), True),
    StructField("language",        StringType(), True),
    StructField("types",           StringType(), True),
    StructField("attributes",      StringType(), True),
    StructField("isOriginalTitle", StringType(), True),
])

# ── Read helper ────────────────────────────────────────────────────────────────

def resolve_path(base: str) -> str:
    """
    Return the actual file path for a given base TSV path.
    Tries:  <base>.tsv  →  <base>.tsv.gz  →  raises FileNotFoundError
    PySpark's CSV reader transparently decompresses gzip when the path ends
    in .gz, so both formats are handled identically after this lookup.
    """
    from pathlib import Path as _Path
    p = _Path(base)
    if p.exists():
        return base
    gz = _Path(base + ".gz")
    if gz.exists():
        return str(gz)
    raise FileNotFoundError(
        f"Neither '{base}' nor '{base}.gz' found.\n"
        "Run 'make download' to fetch the dataset."
    )


def read_tsv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """
    Read an IMDb tab-separated file (plain or gzip) with correct options:
      • sep=\\t          — tab-delimited columns
      • nullValue=\\N    — IMDb's missing-value sentinel
      • encoding=UTF-8  — character set per IMDb spec
      • quote=""        — no quoting in IMDb files (avoids misparse on titles with quotes)
    PySpark auto-detects gzip from the .gz extension and decompresses on the fly.
    """
    actual_path = resolve_path(path)
    return (
        spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("nullValue", "\\N")
        .option("quote", "")
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv(actual_path)
    )

# ── Transform: titles ─────────────────────────────────────────────────────────

def transform_basics(basics: DataFrame) -> DataFrame:
    return (
        basics
        .withColumn(
            "titleType",
            F.when(F.col("titleType").isNull() | (F.trim(F.col("titleType")) == ""), F.lit("unknown"))
             .otherwise(F.col("titleType"))
        )
        .withColumn("startYear",      F.col("startYear").cast(IntegerType()))
        .withColumn("endYear",        F.col("endYear").cast(IntegerType()))
        .withColumn("runtimeMinutes", F.col("runtimeMinutes").cast(IntegerType()))
        .withColumn("isAdult",        F.col("isAdult").cast(IntegerType()))
        # "Action,Drama,Thriller" → ["Action","Drama","Thriller"]
        .withColumn("genres_arr",     F.split(F.col("genres"), ","))
        # decade: 1994 → 1990
        .withColumn(
            "decade",
            F.when(
                F.col("startYear").isNotNull(),
                (F.col("startYear") / 10).cast(IntegerType()) * 10,
            )
        )
        # First genre element; null if empty
        .withColumn(
            "primaryGenre",
            F.when(
                (F.size(F.col("genres_arr")) > 0) & (F.col("genres_arr").getItem(0) != ""),
                F.col("genres_arr").getItem(0),
            )
        )
        .drop("genres")
        .withColumnRenamed("genres_arr", "genres")
    )


def transform_ratings(ratings: DataFrame) -> DataFrame:
    return (
        ratings
        .withColumn("averageRating", F.col("averageRating").cast(FloatType()))
        .withColumn("numVotes",      F.col("numVotes").cast(IntegerType()))
    )


def build_titles_df(basics: DataFrame, ratings: DataFrame) -> DataFrame:
    """
    Join cleaned basics with ratings → one row per title.

    `title.ratings.tsv` is much smaller than `title.basics.tsv`, so we have used broadcast join.
    That avoids a large shuffle while writing titles, which is important on a
    memory-constrained local Spark worker.
    """
    return (
        basics
        .join(F.broadcast(ratings), on="tconst", how="left")
        .select(
            "tconst", "titleType", "primaryTitle", "originalTitle",
            "isAdult", "startYear", "endYear", "runtimeMinutes",
            "genres", "primaryGenre", "decade",
            "averageRating", "numVotes",
        )
    )

# ── Transform: people ─────────────────────────────────────────────────────────

def transform_people(name_basics: DataFrame) -> DataFrame:
    """
    Clean name.basics and derive a partition key from the first profession.
    primaryProfession is a comma-separated string: "actor,producer,director"
    """
    df = (
        name_basics
        .withColumn("birthYear",  F.col("birthYear").cast(IntegerType()))
        .withColumn("deathYear",  F.col("deathYear").cast(IntegerType()))
        # Split comma-separated strings → arrays
        .withColumn("primaryProfession", F.split(F.col("primaryProfession"), ","))
        .withColumn("knownForTitles",    F.split(F.col("knownForTitles"),    ","))
        # Partition key: first listed profession (actor, director, writer, …)
        .withColumn(
            "primaryProfession0",
            F.when(
                (F.size(F.col("primaryProfession")) > 0) &
                (F.col("primaryProfession").getItem(0) != ""),
                F.col("primaryProfession").getItem(0),
            ).otherwise(F.lit("unknown"))
        )
        .filter(F.col("nconst").isNotNull())
    )
    return df.select(
        "nconst", "primaryName", "birthYear", "deathYear",
        "primaryProfession", "knownForTitles", "primaryProfession0",
    )

# ── Transform: principals ─────────────────────────────────────────────────────

def transform_principals(principals: DataFrame) -> DataFrame:
    """
    Clean title.principals.
    category examples: actor, actress, director, writer, producer,
                       cinematographer, composer, editor, production_designer, self
    """
    return (
        principals
        .withColumn("ordering", F.col("ordering").cast(IntegerType()))
        .withColumn(
            "category",
            F.when(F.col("category").isNull(), F.lit("unknown"))
             .otherwise(F.col("category"))
        )
        .filter(F.col("tconst").isNotNull() & F.col("nconst").isNotNull())
        .select("tconst", "ordering", "nconst", "category", "job", "characters")
    )

# ── Transform: akas ───────────────────────────────────────────────────────────

def transform_akas(akas: DataFrame) -> DataFrame:
    """
    Clean title.akas (alternate titles by region/language).
    region is a two-letter ISO code; empty/null → 'XX' (unknown).
    """
    return (
        akas
        .withColumn("ordering",        F.col("ordering").cast(IntegerType()))
        .withColumn("isOriginalTitle", F.col("isOriginalTitle").cast(IntegerType()))
        # Split pipe-separated types/attributes strings → arrays
        .withColumn("types",      F.split(F.coalesce(F.col("types"),      F.lit("")), ","))
        .withColumn("attributes", F.split(F.coalesce(F.col("attributes"), F.lit("")), ","))
        # Normalise null/empty regions so the partition column is never null
        .withColumn(
            "region",
            F.when(F.col("region").isNull() | (F.col("region") == ""), F.lit("XX"))
             .otherwise(F.col("region"))
        )
        .withColumn(
            "language",
            F.when(F.col("language").isNull() | (F.col("language") == ""), F.lit(""))
             .otherwise(F.col("language"))
        )
        .filter(F.col("titleId").isNotNull())
        .select(
            "titleId", "ordering", "title", "region", "language",
            "types", "attributes", "isOriginalTitle",
        )
    )

# ── Write helper ──────────────────────────────────────────────────────────────

def reset_output_path(path: str) -> None:
    """Remove an output directory before an append-based chunked write."""
    p = Path(path)
    if p.exists():
        shutil.rmtree(p)


def write_parquet(df: DataFrame, path: str, partition_cols: list, mode: str = "overwrite") -> None:
    """Write a DataFrame as Snappy-compressed, partitioned Parquet."""
    (
        df.write
        .mode(mode)
        .option("compression", "snappy")
        .option("maxRecordsPerFile", 250_000)
        .partitionBy(*partition_cols)
        .parquet(path)
    )


def write_titles_chunked(titles_df: DataFrame, path: str) -> None:
    """
    Low-memory title writer.

    A single `partitionBy("titleType", "decade")` over the full titles dataset
    can keep too many partition writers and shuffle buffers alive on a small
    local Docker worker. Instead, write one titleType slice at a time. This is
    slower because Spark scans the source more than once, but each job has much
    lower peak memory.
    """
    reset_output_path(path)

    title_types = [
        row.titleType
        for row in titles_df.select("titleType").distinct().orderBy("titleType").collect()
    ]

    for title_type in title_types:
        chunk = titles_df.filter(F.col("titleType") == title_type)
        write_parquet(chunk, path, ["titleType", "decade"], mode="append")

# ── Args ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="IMDb PySpark ETL — all 5 files")
    p.add_argument("--input",  default="/data/raw",  help="Directory with TSV files")
    p.add_argument("--output", default="/data/lake", help="Root for Parquet lake output")
    p.add_argument(
        "--dataset",
        choices=["all", "titles", "people", "principals", "akas"],
        default="all",
        help="Dataset to write. Use with --title-type for low-memory title chunks.",
    )
    p.add_argument(
        "--title-type",
        help="When --dataset=titles, write only this titleType chunk in append mode.",
    )
    p.add_argument("--shuffle-partitions", type=int, default=64,
                   help="spark.sql.shuffle.partitions (tune to 2× executor cores)")
    return p.parse_args()

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args       = parse_args()
    input_dir  = args.input.rstrip("/")
    output_dir = args.output.rstrip("/")

    log.info(f"Running ETL: dataset={args.dataset}, input={input_dir}, output={output_dir}")

    spark = (
        SparkSession.builder
        .appName("IMDb-ETL")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.files.maxPartitionBytes", "4m")
        .config("spark.sql.autoBroadcastJoinThreshold", "128m")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    t0 = time.time()

    # Read and write only what this invocation needs. The Makefile runs several
    # small Spark applications locally so JVM memory is released between chunks.
    if args.dataset in ("all", "titles"):
        basics_raw  = read_tsv(spark, f"{input_dir}/title.basics.tsv",  BASICS_SCHEMA)
        ratings_raw = read_tsv(spark, f"{input_dir}/title.ratings.tsv", RATINGS_SCHEMA)
        titles_df = build_titles_df(transform_basics(basics_raw), transform_ratings(ratings_raw))

        titles_out = f"{output_dir}/titles"
        if args.title_type:
            chunk = titles_df.filter(F.col("titleType") == args.title_type)
            write_parquet(chunk, titles_out, ["titleType", "decade"], mode="append")
        else:
            write_titles_chunked(titles_df, titles_out)

    if args.dataset in ("all", "people"):
        names_raw = read_tsv(spark, f"{input_dir}/name.basics.tsv", NAME_BASICS_SCHEMA)
        people_df = transform_people(names_raw)
        write_parquet(people_df, f"{output_dir}/people", ["primaryProfession0"])

    if args.dataset in ("all", "principals"):
        principals_raw = read_tsv(spark, f"{input_dir}/title.principals.tsv", PRINCIPALS_SCHEMA)
        principals_df = transform_principals(principals_raw)
        write_parquet(principals_df, f"{output_dir}/principals", ["category"])

    if args.dataset in ("all", "akas"):
        akas_raw = read_tsv(spark, f"{input_dir}/title.akas.tsv", AKAS_SCHEMA)
        akas_df = transform_akas(akas_raw)
        write_parquet(akas_df, f"{output_dir}/akas", ["region"])

    elapsed = time.time() - t0
    spark.stop()
    log.info(f"ETL complete in {elapsed:.1f}s. Next step: make load")


if __name__ == "__main__":
    main()
