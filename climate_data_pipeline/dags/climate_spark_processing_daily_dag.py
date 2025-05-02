"""
DAG: climate_data_spark_processing
Descrição : Processa dados climáticos + World Bank com Spark e publica no BigQuery.
Autor      : zoomcamp
Atualizado : 2025‑05‑02
"""

from __future__ import annotations

import os
from datetime import timedelta, datetime

import pendulum
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.sensors.external_task import ExternalTaskSensorAsync
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator

import importlib

spark_mod = importlib.import_module("spark_processor")
process_world_bank_data = spark_mod.process_world_bank_data
process_climate_trace_data = spark_mod.process_climate_trace_data
combine_datasets = spark_mod.combine_datasets

LOCAL_TZ = pendulum.timezone("America/Sao_Paulo")

DEFAULT_ARGS: dict[str, object] = {
    "owner": "zoomcamp",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

YEAR_DEFAULT = Variable.get("extraction_year", default_var=str(datetime.now().year))

GCS_BUCKET = "zoomcamp-climate-trace"
BQ_DATASET = "zoomcamp_climate_raw"

# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="climate_data_spark_processing",
    description="Process climate + World Bank data with Spark then publish to BigQuery",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["climate_data", "spark", "bigquery"],
    params={"year": YEAR_DEFAULT},
)
def spark_processing_pipeline():
    year = "{{ params.year }}"
    gcs_processed_prefix = f"gs://{GCS_BUCKET}/processed"

    paths = {
        "wb_raw": f"gs://{GCS_BUCKET}/world_bank/world_bank_indicators_{year}.csv",
        "ct_raw": f"gs://{GCS_BUCKET}/climate_trace/global_emissions_{year}.csv",
        "wb_processed": f"{gcs_processed_prefix}/world_bank/{year}",
        "ct_processed": f"{gcs_processed_prefix}/climate_trace/{year}",
        "combined": f"{gcs_processed_prefix}/combined/{year}",
    }

    wait_for_extraction = ExternalTaskSensorAsync(
        task_id="wait_for_extraction",
        external_dag_id="climate_data_pipeline",
        external_task_id=None, 
        timeout=10 * 60,
        poke_interval=60,
        deferrable=True,
    )

    process_wb = PythonOperator(
        task_id="process_world_bank",
        python_callable=process_world_bank_data,
        op_kwargs={
            "input_path": paths["wb_raw"],
            "output_path": paths["wb_processed"],
        },
    )

    process_ct = PythonOperator(
        task_id="process_climate_trace",
        python_callable=process_climate_trace_data,
        op_kwargs={
            "input_path": paths["ct_raw"],
            "output_path": paths["ct_processed"],
        },
    )

    combine = PythonOperator(
        task_id="combine",
        python_callable=combine_datasets,
        op_kwargs={
            "world_bank_path": paths["wb_processed"],
            "climate_trace_path": paths["ct_processed"],
            "output_path": paths["combined"],
        },
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    def bq_external(task_id, table_id, source_uri, extra_conf=None):
        conf = {
            "sourceFormat": "PARQUET",
            "sourceUris": [f"{source_uri}/*.parquet"],
            "autodetect": True,
        }
        if extra_conf:
            conf.update(extra_conf)

        return BigQueryCreateExternalTableOperator(
            task_id=task_id,
            table_resource={
                "tableReference": {
                    "projectId": "{{ var.value.gcp_project }}",
                    "datasetId": BQ_DATASET,
                    "tableId": table_id,
                },
                "externalDataConfiguration": conf,
            },
            trigger_rule=TriggerRule.ONE_SUCCESS,
        )

    create_wb_table = bq_external(
        "create_world_bank_table",
        f"world_bank_indicators_{year}",
        paths["wb_processed"],
    )

    create_ct_table = bq_external(
        "create_climate_trace_table",
        f"climate_trace_emissions_{year}",
        paths["ct_processed"],
    )

    create_combined_table = bq_external(
        "create_combined_table",
        f"combined_climate_economic_{year}",
        paths["combined"],
        extra_conf={
            "hivePartitioningOptions": {
                "mode": "CUSTOM",
                "sourceUriPrefix": f"{paths['combined']}/",
                "fields": ["year"],
            },
            "clustering": {"fields": ["country", "region"]},
        },
    )

    wait_for_extraction >> [process_wb, process_ct]
    [process_wb, process_ct] >> combine
    process_wb >> create_wb_table
    process_ct >> create_ct_table
    combine >> create_combined_table


dag = spark_processing_pipeline()
