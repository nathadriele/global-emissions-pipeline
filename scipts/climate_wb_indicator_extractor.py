"""
Download selected World‑Bank indicators and save one tidy CSV per year.

Example
-------
$ python wb_indicator_extractor.py 2015 --end_year 2020 --out_dir data
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
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


WB_BASE = "https://api.worldbank.org/v2"

INDICATORS: Dict[str, str] = {
    "SP.POP.TOTL": "Population, total",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "SI.POV.GAPS": "Poverty gap @ $2.15 (2017 PPP)",
    "SP.DYN.LE00.IN": "Life expectancy (yrs)",
    "SE.SEC.ENRR": "School enrolment, secondary % gross",
    "SI.POV.GINI": "Gini index",
    "SL.UEM.TOTL.ZS": "Unemployment % labour force",
}


@dataclass(slots=True)
class ExtractorCfg:
    out_dir: Path = Path(os.getenv("WB_OUT_DIR", "world_bank_data"))
    max_retries: int = int(os.getenv("WB_MAX_RETRIES", 3))
    backoff: float = float(os.getenv("WB_BACKOFF", 0.7))
    timeout: int = int(os.getenv("WB_TIMEOUT", 30))
    pause: float = float(os.getenv("WB_SLEEP", 0.4))  # between indicators


def build_session(cfg: ExtractorCfg) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=cfg.max_retries,
        backoff_factor=cfg.backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_indicator_year(
    sess: requests.Session, indicator: str, year: int, cfg: ExtractorCfg
) -> Dict[str, Any]:
    url = f"{WB_BASE}/countries/all/indicators/{indicator}"
    params = {"date": year, "format": "json", "per_page": 400}

    try:
        resp = sess.get(url, params=params, timeout=cfg.timeout)
        resp.raise_for_status()
        rows = resp.json()[1]]
    except Exception as exc:
        logging.warning("Failed %s %s – %s", indicator, year, exc)
        return {}

    return {
        r["countryiso3code"]: r["value"]
        for r in rows
        if r["value"] is not None and r["countryiso3code"]
    }


def build_year_dataframe(
    sess: requests.Session, year: int, cfg: ExtractorCfg
) -> pd.DataFrame:
    country_data: Dict[str, Dict[str, Any]] = {}

    for code, name in tqdm(INDICATORS.items(), desc=f"Year {year}"):
        data = fetch_indicator_year(sess, code, year, cfg)
        for iso, val in data.items():
            country_data.setdefault(iso, {"country": iso})[code] = val
        time.sleep(cfg.pause)

    return pd.DataFrame(country_data.values())


def save_year_csv(df: pd.DataFrame, year: int, cfg: ExtractorCfg) -> Path:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    file = cfg.out_dir / f"world_bank_indicators_{year}.csv"
    df.to_csv(file, index=False, quoting=csv.QUOTE_MINIMAL)
    logging.info("Saved %d rows → %s", len(df), file)
    return file


def run(start: int, end: int | None = None, cfg: ExtractorCfg | None = None) -> None:
    cfg = cfg or ExtractorCfg()
    sess = build_session(cfg)
    end_year = end or start
    logging.info("Extracting %s → %s", start, end_year)

    for yr in range(start, end_year + 1):
        df = build_year_dataframe(sess, yr, cfg)
        save_year_csv(df, yr, cfg)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="World‑Bank indicator extractor")
    p.add_argument("start_year", type=int)
    p.add_argument("--end_year", type=int)
    p.add_argument("--out_dir", type=Path)
    return p.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = ExtractorCfg(out_dir=args.out_dir or ExtractorCfg().out_dir)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    run(args.start_year, args.end_year, cfg)

if __name__ == "__main__":
    main()
