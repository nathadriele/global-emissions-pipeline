"""
Pulls annual GHG‑emissions data from Climate‑Trace and stores per‑year CSVs.

Usage
-----
$ python climate_trace_extractor.py 2015 --to_year 2020
# or override defaults with env vars
$ CT_OUTPUT_DIR=/data/ct python climate_trace_extractor.py 2020
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from tqdm import tqdm
except ImportError:  # optional dependency
    tqdm = lambda x, **k: x  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# Configuration dataclass
# ─────────────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
class CTConfig:
    base_url: str = os.getenv("CT_BASE_URL", "https://api.climatetrace.org/v6")
    out_dir: Path = Path(os.getenv("CT_OUTPUT_DIR", "climate_trace_emissions_data"))
    batch_size: int = int(os.getenv("CT_BATCH_SIZE", 25))
    max_retries: int = int(os.getenv("CT_MAX_RETRIES", 3))
    backoff_factor: float = float(os.getenv("CT_BACKOFF_FACTOR", 0.8))
    timeout: int = int(os.getenv("CT_TIMEOUT", 30))


# ─────────────────────────────────────────────────────────────────────────────
# Extractor
# ─────────────────────────────────────────────────────────────────────────────
class ClimateTraceExtractor:
    def __init__(self, since_year: int, to_year: int | None = None, cfg: CTConfig | None = None) -> None:
        self.cfg = cfg or CTConfig()
        self.since_year = since_year
        self.to_year = to_year or since_year
        self.cfg.out_dir.mkdir(parents=True, exist_ok=True)

        # requests session with robust retry policy
        self.session = requests.Session()
        retries = Retry(
            total=self.cfg.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=self.cfg.backoff_factor,
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)-8s | %(message)s",
            stream=sys.stdout,
        )
        self.log = logging.getLogger("ClimateTraceExtractor")

    # ──────────────────────────────────────────────────────────────────────
    # API helpers
    # ──────────────────────────────────────────────────────────────────────
    def _get_json(self, endpoint: str, params: Dict[str, Any] | None = None) -> Any:
        url = f"{self.cfg.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        self.log.debug("GET %s params=%s", url, params)
        resp = self.session.get(url, params=params, timeout=self.cfg.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_countries(self) -> List[Dict[str, Any]]:
        """Return full country listing (cca3 codes)."""
        return self._get_json("definitions/countries/")

    def fetch_emissions_batch(self, country_codes: List[str], year: int) -> List[Dict[str, Any]]:
        """Fetch one batch of country emissions for a given year."""
        codes = ",".join(country_codes)
        endpoint = "country/emissions"
        params = {"since": year, "to": year + 1, "countries": codes}
        return self._get_json(endpoint, params)

    # ──────────────────────────────────────────────────────────────────────
    # Processing
    # ──────────────────────────────────────────────────────────────────────
    @staticmethod
    def _normalize_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for rec in records:
            country = rec.get("country")
            for typ, val in (rec.get("emissions") or {}).items():
                rows.append({"country": country, "emission_type": typ, "value": val})
        return (
            pd.DataFrame(rows)
            .pivot_table(index="country", columns="emission_type", values="value")
            .reset_index()
            .rename_axis(None, axis=1)
        )

    def save_year(self, year: int, df: pd.DataFrame) -> None:
        if df.empty:
            self.log.warning("No data for %s – skipping save", year)
            return
        file = self.cfg.out_dir / f"global_emissions_{year}.csv"
        df.to_csv(file, index=False, quoting=csv.QUOTE_MINIMAL)
        self.log.info("Saved %d rows to %s", len(df), file)

    # ──────────────────────────────────────────────────────────────────────
    # Public pipeline
    # ──────────────────────────────────────────────────────────────────────
    def run(self) -> None:
        countries = [c["alpha3"] for c in self.get_countries()]
        self.log.info("Fetched %d country codes", len(countries))

        for year in range(self.since_year, self.to_year + 1):
            self.log.info("=== Processing year %s ===", year)
            all_records: List[Dict[str, Any]] = []

            for i in tqdm(range(0, len(countries), self.cfg.batch_size), desc=f"Year {year}"):
                batch = countries[i : i + self.cfg.batch_size]
                try:
                    batch_records = self.fetch_emissions_batch(batch, year)
                    all_records.extend(batch_records)
                except requests.HTTPError as e:
                    self.log.error("HTTP error for batch %s – %s", batch, e)
                except Exception as e:  # pylint: disable=broad-except
                    self.log.exception("Unexpected error in batch %s: %s", batch, e)

            df = self._normalize_records(all_records)
            self.save_year(year, df)


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry‑point
# ─────────────────────────────────────────────────────────────────────────────
def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Climate‑Trace emissions data.")
    parser.add_argument("since_year", type=int, help="First year to include (YYYY)")
    parser.add_argument("--to_year", type=int, help="Last year to include (inclusive)")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    extractor = ClimateTraceExtractor(args.since_year, args.to_year)
    extractor.run()


if __name__ == "__main__":
    main()
