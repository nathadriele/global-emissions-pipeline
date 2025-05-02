from __future__ import annotations

"""Airflow DAG – Climate & Socio‑Economic data multi‑year pipeline.

Improvements applied:
* Typed, Pathlib‑based paths.
* Robust parsing of `extraction_years` Airflow Variable.
* Configurable GCS bucket via Variable (`gcs_raw_bucket`).
* Consolidated retries & logging; `@quarterly` schedule.
* `max_active_runs=1` to avoid overlapping quarters.
* No hard‑coded sys.path; scripts dir injected via Pathlib.
"""

import json
import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

# ---------------------------------------------------------------------------
# Local imports – makes the extractor package‑agnostic & testable
# ---------------------------------------------------------------------------
SCRIPTS_DIR = Path(os.environ.get("AIRFLOW_HOME", "")) / "scripts"
os.sys.path.append(str(SCRIPTS_DIR))
from data_extractor import (
    run_climate_trace_pipeline,
    run_world_bank_pipeline,
)

# ---------------------------------------------------------------------------
# General settings
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

DEFAULT_ARGS: dict[str, object] = {
    "owner": "zoomcamp",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", ""))
DATA_DIR = AIRFLOW_HOME / "data"
WORLD_BANK_DIR = DATA_DIR / "world_bank"
CLIMATE_TRACE_DIR = DATA_DIR / "climate_trace"

GCS_BUCKET: str = Variable.get("gcs_raw_bucket", default_var="zoomcamp-climate-trace")

# ---------------------------------------------------------------------------
# Helper – parse years list from Airflow Variable
# ---------------------------------------------------------------------------

def _parse_extraction_years() -> list[int]:
    """Return list of years to process, defaulting to *current* year."""
    current_year = pendulum.now().year
    raw_value = Variable.get("extraction_years", default_var=str(current_year))

    try:
        years = json.loads(raw_value) if raw_value.startswith("[") else [int(raw_value)]
    except (json.JSONDecodeError, ValueError):
        logger.warning("Invalid `extraction_years` Airflow Variable: %s. Fallback to current year.", raw_value)
        years = [current_year]

    valid_years: list[int] = []
    for y in years:
        try:
            valid_years.append(int(y))
        except Exception:  # noqa: BLE001
            logger.warning("Skipping invalid year value: %s", y)
    return valid_years or [current_year]


EXTRACTION_YEARS = _parse_extraction_years()
logger.info("Pipeline will process years: %s", EXTRACTION_YEARS)

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="climate_data_pipeline_multi_year",
    description="Extract Climate Trace & World Bank data and stage on GCS (quarterly)",
    default_args=DEFAULT_ARGS,
    schedule_interval="@quarterly",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["climate_data"],
) as dag:

    for year in EXTRACTION_YEARS:
        # -------------------------------------------------------------------
        # Extraction tasks
        # -------------------------------------------------------------------
        extract_wb = PythonOperator(
            task_id=f"extract_world_bank_data_{year}",
            python_callable=run_world_bank_pipeline,
            op_kwargs={"year": year, "output_dir": str(WORLD_BANK_DIR)},
        )

        extract_ct = PythonOperator(
            task_id=f"extract_climate_trace_data_{year}",
            python_callable=run_climate_trace_pipeline,
            op_kwargs={"year": year, "output_dir": str(CLIMATE_TRACE_DIR)},
        )

        # -------------------------------------------------------------------
        # Load → GCS tasks
        # -------------------------------------------------------------------
        upload_wb = LocalFilesystemToGCSOperator(
            task_id=f"upload_world_bank_to_gcs_{year}",
            src=str(WORLD_BANK_DIR / f"world_bank_indicators_{year}.csv"),
            dst=f"world_bank/world_bank_indicators_{year}.csv",
            bucket=GCS_BUCKET,
            gcp_conn_id="google_cloud_default",
        )

        upload_ct = LocalFilesystemToGCSOperator(
            task_id=f"upload_climate_trace_to_gcs_{year}",
            src=str(CLIMATE_TRACE_DIR / f"global_emissions_{year}.csv"),
            dst=f"climate_trace/global_emissions_{year}.csv",
            bucket=GCS_BUCKET,
            gcp_conn_id="google_cloud_default",
        )

        extract_wb >> upload_wb
        extract_ct >> upload_ct
