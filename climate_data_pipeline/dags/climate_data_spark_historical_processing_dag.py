"""
DAG: climate_data_spark_historical_data_processing
Processes (with Spark) historical data from the World Bank and ClimateTrace
and creates external tables in BigQuery.

Steps:
1. Checks if the CSV files exist in GCS.
2. Processes each dataset with Spark (if it exists).
3. Combines the two datasets if they both exist.
4. Creates external tables in BigQuery for each generated parquet.
Author: zoomcamp
Updated: 2025‑05‑02
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# ──────────────────────────────────────────────────────────────────────────────
# Configurações
# ──────────────────────────────────────────────────────────────────────────────
LOCAL_TZ = pendulum.timezone("America/Sao_Paulo")

DEFAULT_ARGS: dict[str, object] = {
    "owner": "zoomcamp",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
SPARK_SCRIPTS_DIR = os.path.join(AIRFLOW_HOME, "scripts")

import importlib

process_world_bank_data = importlib.import_module("spark_processor").process_world_bank_data
process_climate_trace_data = importlib.import_module("spark_processor").process_climate_trace_data
combine_datasets = importlib.import_module("spark_processor").combine_datasets

PROCESSING_YEAR_DEFAULT = Variable.get("processing_year", default_var="2025")

GCS_BUCKET = "zoomcamp-climate-trace"
BQ_DATASET = "zoomcamp_climate_raw"

# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="climate_data_spark_historical_data_processing",
    description="Process climate + economic data with Spark and publish to BigQuery",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["climate_data", "spark", "bigquery"],
    params={"year": PROCESSING_YEAR_DEFAULT},
)
def spark_historical_pipeline():
    year = "{{ params.year }}"

    gcs_processed_prefix = f"gs://{GCS_BUCKET}/processed"
    paths = {
        "world_bank_raw": f"gs://{GCS_BUCKET}/world_bank/world_bank_indicators_{year}.csv",
        "climate_trace_raw": f"gs://{GCS_BUCKET}/climate_trace/global_emissions_{year}.csv",
        "world_bank_processed": f"{gcs_processed_prefix}/world_bank/{year}",
        "climate_trace_processed": f"{gcs_processed_prefix}/climate_trace/{year}",
        "combined": f"{gcs_processed_prefix}/combined/{year}",
    }

    start = EmptyOperator(task_id="start")

    @task.branch()
    def detect_sources() -> list[str]:
        """Return list of downstream task_ids to execute."""
        hook = GCSHook()
        world_bank_exists = hook.exists(
            bucket_name=GCS_BUCKET, object_name=f"world_bank/world_bank_indicators_{year}.csv"
        )
        climate_trace_exists = hook.exists(
            bucket_name=GCS_BUCKET, object_name=f"climate_trace/global_emissions_{year}.csv"
        )

        selected: list[str] = []
        if world_bank_exists:
            selected.append("process_world_bank")
        if climate_trace_exists:
            selected.append("process_climate_trace")
        if world_bank_exists and climate_trace_exists:
            selected.append("combine")
        return selected or ["no_data_found"]

    process_world_bank = (
        importlib.import_module("airflow.operators.python").PythonOperator(
            task_id="process_world_bank",
            python_callable=process_world_bank_data,
            op_kwargs={
                "input_path": paths["world_bank_raw"],
                "output_path": paths["world_bank_processed"],
            },
        )
    )

    process_climate_trace = (
        importlib.import_module("airflow.operators.python").PythonOperator(
            task_id="process_climate_trace",
            python_callable=process_climate_trace_data,
            op_kwargs={
                "input_path": paths["climate_trace_raw"],
                "output_path": paths["climate_trace_processed"],
            },
        )
    )

    combine = (
        importlib.import_module("airflow.operators.python").PythonOperator(
            task_id="combine",
            python_callable=combine_datasets,
            op_kwargs={
                "world_bank_path": paths["world_bank_processed"],
                "climate_trace_path": paths["climate_trace_processed"],
                "output_path": paths["combined"],
            },
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )
    )

    no_data_found = EmptyOperator(task_id="no_data_found")

    def make_bq_operator(task_id, table_id, source_uri) -> BigQueryCreateExternalTableOperator:
        return BigQueryCreateExternalTableOperator(
            task_id=task_id,
            table_resource={
                "tableReference": {
                    "projectId": "{{ var.value.gcp_project }}",
                    "datasetId": BQ_DATASET,
                    "tableId": table_id,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"{source_uri}/*.parquet"],
                    "autodetect": True,
                },
            },
            trigger_rule=TriggerRule.ONE_SUCCESS,
        )

    create_wb_table = make_bq_operator(
        "create_world_bank_table",
        f"world_bank_indicators_{year}",
        paths["world_bank_processed"],
    )

    create_ct_table = make_bq_operator(
        "create_climate_trace_table",
        f"climate_trace_emissions_{year}",
        paths["climate_trace_processed"],
    )

    create_combined_table = make_bq_operator(
        "create_combined_table",
        f"combined_climate_economic_{year}",
        paths["combined"],
    )

    start >> detect_sources()

    detect_sources() >> [process_world_bank, process_climate_trace, combine, no_data_found]

    process_world_bank >> create_wb_table
    process_climate_trace >> create_ct_table
    combine >> create_combined_table

dag = spark_historical_pipeline()
