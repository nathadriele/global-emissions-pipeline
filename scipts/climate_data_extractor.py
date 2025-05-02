from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from tqdm import tqdm
except ImportError:
    tqdm = lambda x, **k: x


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
class APIConfig:
    batch_size: int = int(os.getenv("BATCH_SIZE", 20))
    output_dir: Path = Path(os.getenv("OUT_DIR", "data"))
    max_retries: int = int(os.getenv("MAX_RETRIES", 3))
    backoff: float = float(os.getenv("BACKOFF", 0.7))
    timeout: int = int(os.getenv("TIMEOUT", 30))


WB_INDICATORS: dict[str, str] = {
    "SP.POP.TOTL": "Population, total",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "SI.POV.GAPS": "Poverty gap at $2.15 (2017 PPP) %",
    "SP.DYN.LE00.IN": "Life expectancy at birth (yrs)",
    "SE.SEC.ENRR": "School enrolment, secondary % gross",
    "SI.POV.GINI": "Gini index",
    "SL.UEM.TOTL.ZS": "Unemployment % labour force",
}

WB_BASE = "https://api.worldbank.org/v2"
CT_BASE = "https://api.climatetrace.org/v6"


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────
def build_session(cfg: APIConfig) -> requests.Session:
    retry = Retry(
        total=cfg.max_retries,
        backoff_factor=cfg.backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    sess = requests.Session()
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    return sess


# ─────────────────────────────────────────────────────────────────────────────
# World Bank
# ─────────────────────────────────────────────────────────────────────────────
def wb_country_list(sess: requests.Session) -> List[str]:
    url = f"{WB_BASE}/country"
    resp = sess.get(url, params={"format": "json", "per_page": 400}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return [c["id"] for c in data[1]]


def fetch_wb_year(sess: requests.Session, year: int, cfg: APIConfig) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for code, _name in WB_INDICATORS.items():
        url = f"{WB_BASE}/countries/all/indicators/{code}"
        resp = sess.get(
            url,
            params={"date": year, "format": "json", "per_page": 400},
            timeout=cfg.timeout,
        )
        resp.raise_for_status()
        for item in resp.json()[1]:
            iso = item["countryiso3code"]
            val = item["value"]
            if val is None or not iso:
                continue
            rows.append({"country": iso, code: val, "year": year})
        time.sleep(0.
    df = (
        pd.DataFrame(rows)
        .groupby(["country", "year"], as_index=False)
        .first()
        .reset_index(drop=True)
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Climate Trace
# ─────────────────────────────────────────────────────────────────────────────
def ct_country_list(sess: requests.Session) -> List[str]:
    resp = sess.get(f"{CT_BASE}/definitions/countries/", timeout=30)
    resp.raise_for_status()
    return [c["alpha3"] for c in resp.json()]


def fetch_ct_year(sess: requests.Session, year: int, cfg: APIConfig) -> pd.DataFrame:
    countries = ct_country_list(sess)
    records: list[dict[str, Any]] = []
    for i in tqdm(range(0, len(countries), cfg.batch_size), desc=f"Year {year}"):
        batch = countries[i : i + cfg.batch_size]
        url = (
            f"{CT_BASE}/country/emissions"
            f"?since={year}&to={year+1}&countries={','.join(batch)}"
        )
        resp = sess.get(url, timeout=cfg.timeout)
        if resp.status_code != 200:
            logging.warning("Skipping batch %s (%s)", batch, resp.status_code)
            continue
        for rec in resp.json():
            iso = rec.get("country")
            for typ, val in (rec.get("emissions") or {}).items():
                records.append({"country": iso, typ: val, "year": year})
        time.sleep(0.3)
    df = (
        pd.DataFrame(records)
        .groupby(["country", "year"], as_index=False)
        .first()
        .reset_index(drop=True)
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Save utility
# ─────────────────────────────────────────────────────────────────────────────
def save_df(df: pd.DataFrame, fname: Path) -> Path:
    fname.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(fname, index=False, quoting=csv.QUOTE_MINIMAL)
    logging.info("Saved %d rows → %s", len(df), fname)
    return fname


# ─────────────────────────────────────────────────────────────────────────────
# CLI & main driver
# ─────────────────────────────────────────────────────────────────────────────
def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract World Bank or Climate Trace data")
    p.add_argument("source", choices=["world_bank", "climate_trace"])
    p.add_argument("since_year", type=int)
    p.add_argument("--to_year", type=int)
    p.add_argument("--out_dir", type=Path, default=APIConfig().output_dir)
    return p.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = APIConfig(output_dir=args.out_dir)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    sess = build_session(cfg)

    to_year = args.to_year or args.since_year
    for yr in range(args.since_year, to_year + 1):
        if args.source == "world_bank":
            df = fetch_wb_year(sess, yr, cfg)
            fname = cfg.output_dir / f"world_bank_indicators_{yr}.csv"
        else:
            df = fetch_ct_year(sess, yr, cfg)
            fname = cfg.output_dir / f"global_emissions_{yr}.csv"
        save_df(df, fname)


if __name__ == "__main__":
    main()
