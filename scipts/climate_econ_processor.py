# climate_econ_processor.py
"""
Transform CSVs from World Bank & Climate‑Trace to partitioned Parquet
and combine them by (country, year).

Functions are stateless and rely on gcs_io helpers for storage.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import List

import pandas as pd

import gcs_io

_LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")


# ─────────────────────────────────────────────────────────────────────────────
# Generic helpers
# ─────────────────────────────────────────────────────────────────────────────
def _tmp_dir() -> tempfile.TemporaryDirectory:
    """Context manager that auto‑cleans."""
    return tempfile.TemporaryDirectory(prefix="ct_wb_")


def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(".", "_")
    )
    return df


def _to_parquet(df: pd.DataFrame, dest_gcs: str) -> str:
    with _tmp_dir() as tmp:
        tmp_file = Path(tmp) / "data.parquet"
        df.to_parquet(tmp_file, index=False)
        return gcs_io.upload(tmp_file, dest_gcs)


def _year_from_name(path: str) -> str:
    return Path(path).stem.split("_")[-1]


# ─────────────────────────────────────────────────────────────────────────────
# World Bank
# ─────────────────────────────────────────────────────────────────────────────
def process_world_bank_csv(src_uri: str, out_prefix: str) -> str:
    """
    Convert a World Bank CSV to Parquet, clean columns and add `year`.
    *out_prefix* should be a directory‑like gs://…/world_bank
    """
    year = _year_from_name(src_uri)
    with _tmp_dir() as tmp:
        local_csv = gcs_io.download(src_uri, Path(tmp) / "wb.csv")
        df = pd.read_csv(local_csv)
        df = _clean_cols(df)
        for c in df.columns:
            if c not in ["country", "year"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        if "year" not in df.columns:
            df["year"] = int(year)
        if {"sp_pop_totl", "en_atm_co2e_pc"}.issubset(df.columns):
            df["total_emissions"] = df["sp_pop_totl"] * df["en_atm_co2e_pc"]

    dest = f"{out_prefix}/{year}/data.parquet"
    return _to_parquet(df, dest)


# ─────────────────────────────────────────────────────────────────────────────
# Climate Trace
# ─────────────────────────────────────────────────────────────────────────────
def process_climate_csv(src_uri: str, out_prefix: str) -> str:
    year = _year_from_name(src_uri)
    with _tmp_dir() as tmp:
        local_csv = gcs_io.download(src_uri, Path(tmp) / "ct.csv")
        df = pd.read_csv(local_csv)
        df = _clean_cols(df)
        numeric_cols = [c for c in df.columns if c not in {"country", "year"}]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        if "year" not in df.columns:
            df["year"] = int(year)
        # aggregate
        df = (
            df.groupby(["country", "year"], as_index=False)
            .agg("sum")
            .reset_index(drop=True)
        )

    dest = f"{out_prefix}/{year}/data.parquet"
    return _to_parquet(df, dest)


# ─────────────────────────────────────────────────────────────────────────────
# Combine
# ─────────────────────────────────────────────────────────────────────────────
def _collect_year_parquets(prefix: str) -> List[str]:
    """Return list of Parquet URIs under prefix/**/data.parquet."""
    return [p for p in gcs_io.list_blobs(prefix) if p.endswith("data.parquet")]


def combine_by_year(wb_prefix: str, ct_prefix: str, out_uri: str) -> str:
    """
    Merge every year available in either dataset (outer join on country+year)
    and upload a single Parquet file to *out_uri*/combined_data.parquet
    """
    wb_files = _collect_year_parquets(wb_prefix)
    ct_files = _collect_year_parquets(ct_prefix)

    def read_many(uris: List[str]) -> pd.DataFrame:
        dfs = []
        for u in uris:
            with _tmp_dir() as tmp:
                dfs.append(pd.read_parquet(gcs_io.download(u, Path(tmp) / "x.parquet")))
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    wb_df, ct_df = read_many(wb_files), read_many(ct_files)
    if not wb_df.empty:
        wb_df = _clean_cols(wb_df)
    if not ct_df.empty:
        ct_df = _clean_cols(ct_df)

    if wb_df.empty and ct_df.empty:
        raise ValueError("No data found in either prefix")

    if wb_df.empty:
        merged = ct_df
    elif ct_df.empty:
        merged = wb_df
    else:
        merged = pd.merge(wb_df, ct_df, on=["country", "year"], how="outer", suffixes=("_wb", "_ct"))

    return _to_parquet(merged, f"{out_uri.rstrip('/')}/combined_data.parquet")
