# climate_spark_transformer.py
"""
Transform World‑Bank & Climate‑Trace CSVs into partitioned Parquet
and combine them for analytics in BigQuery / dbt.

All paths are gs:// URIs and require the GCS connector jars.
"""

from __future__ import annotations

import logging
import os
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
LOG = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Spark helpers
# -----------------------------------------------------------------------------
GCS_JARS = (
    "org.apache.hadoop:hadoop-gcs:3.3.2,"
    "com.google.cloud:google-cloud-storage:2.2.0"
)
_DEFAULT_MASTER = os.getenv("SPARK_MASTER", "local[*]")


def get_spark(app: str) -> SparkSession:
    """Build (or fetch) a configured SparkSession."""
    spark = (
        SparkSession.builder.appName(app)
        .master(_DEFAULT_MASTER)
        .config("spark.jars.packages", GCS_JARS)
        .getOrCreate()
    )

    # Configure GCS connector – service‑account picked from env var
    gcs_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gcs_key:
        hconf = spark._jsc.hadoopConfiguration()
        hconf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        hconf.set(
            "fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        hconf.set("fs.gs.auth.service.account.enable", "true")
        hconf.set("google.cloud.auth.service.account.json.keyfile", gcs_key)

    return spark


# -----------------------------------------------------------------------------
# Common transformations
# -----------------------------------------------------------------------------
def _clean_columns(df: DataFrame, keep: List[str]) -> DataFrame:
    for c in df.columns:
        if c not in keep:
            df = df.withColumn(c, F.col(c).cast(DoubleType()))
    return df


def _partition_path(prefix: str, year: str) -> str:
    prefix = prefix.rstrip("/")
    # e.g., gs://bucket/processed/world_bank/year=2025
    return f"{prefix}/year={year}"


# -----------------------------------------------------------------------------
# World Bank
# -----------------------------------------------------------------------------
def process_world_bank_data(src_csv: str, dst_prefix: str) -> str:
    """
    Convert World‑Bank CSV → Parquet partitioned by year.

    Returns the GCS folder (not the file) where Parquet is written.
    """
    spark = get_spark("World‑Bank Transform")

    year = os.path.basename(src_csv).split(".")[0].split("_")[-1]
    LOG.info("Processing World Bank year %s", year)

    df = spark.read.options(header=True).csv(src_csv)
    df = (
        _clean_columns(df, keep=["country", "year"])
        .withColumn("year", F.lit(int(year)))
        .withColumnRenamed("SP_POP_TOTL", "sp_pop_totl")
        .withColumnRenamed("EN_ATM_CO2E_PC", "en_atm_co2e_pc")
    )

    if {"sp_pop_totl", "en_atm_co2e_pc"}.issubset(df.columns):
        df = df.withColumn("total_emissions", F.col("sp_pop_totl") * F.col("en_atm_co2e_pc"))

    out_dir = _partition_path(dst_prefix, year)
    df.write.mode("overwrite").parquet(out_dir)
    LOG.info("World Bank parquet written to %s", out_dir)
    return out_dir


# -----------------------------------------------------------------------------
# Climate Trace
# -----------------------------------------------------------------------------
def process_climate_trace_data(src_csv: str, dst_prefix: str) -> str:
    """Clean & aggregate Climate‑Trace CSV, write Parquet partitioned by year."""
    spark = get_spark("Climate‑Trace Transform")

    year = os.path.basename(src_csv).split(".")[0].split("_")[-1]
    LOG.info("Processing Climate‑Trace year %s", year)

    df = spark.read.options(header=True).csv(src_csv)
    df = _clean_columns(df, keep=["country", "year"]).withColumn("year", F.lit(int(year)))

    agg_df = (
        df.groupBy("country", "year")
        .agg(
            F.sum("co2").alias("co2"),
            F.sum("ch4").alias("ch4"),
            F.sum("n2o").alias("n2o"),
            F.sum("co2e_100yr").alias("co2e_100yr"),
            F.sum("co2e_20yr").alias("co2e_20yr"),
            F.sum("total").alias("total"),
            F.sum("energy").alias("energy"),
        )
        .withColumn("energy_percent", F.col("energy") / F.col("total") * 100)
    )

    out_dir = _partition_path(dst_prefix, year)
    agg_df.write.mode("overwrite").parquet(out_dir)
    LOG.info("Climate‑Trace parquet written to %s", out_dir)
    return out_dir


# -----------------------------------------------------------------------------
# Combine datasets
# -----------------------------------------------------------------------------
def combine_datasets(wb_parquet: str, ct_parquet: str, dst_path: str) -> str:
    """
    Join World‑Bank & Climate‑Trace Parquet folders on (country, year)
    and write a single combined Parquet folder.
    """
    spark = get_spark("Combine WB & CT")

    wb_df = spark.read.parquet(wb_parquet)
    ct_df = spark.read.parquet(ct_parquet)

    combined = wb_df.join(
        ct_df,
        on=["country", "year"],
        how="inner", 
    )

    combined.write.mode("overwrite").parquet(dst_path)
    LOG.info("Combined parquet written to %s", dst_path)
    return dst_path
