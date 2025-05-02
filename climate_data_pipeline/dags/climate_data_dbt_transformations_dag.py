"""
DAG: climate_data_dbt_transformations
Purpose : Run dbt models on consolidated climate data in BigQuery
Author  : zoomcamp
Updated : 2025‑05‑02
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensorAsync
from airflow.models import Variable

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
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
DBT_PROJECT_DIR = os.path.join(
    AIRFLOW_HOME,
    "dbt_climate_data",
    "climate_transforms",
)

DEFAULT_YEAR = Variable.get("processing_year", default_var="2025")

# ──────────────────────────────────────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────────────────────────────────────

@dag(
    dag_id="climate_data_dbt_transformations",
    description="Run dbt transformations on combined climate data",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2023, 1, 1, tz=LOCAL_TZ),
    schedule_interval=None,
    catchup=False,
    params={
        "year": DEFAULT_YEAR,
    },
    tags=["climate_data", "dbt", "bigquery"],
)
def climate_data_dbt_transformations():
    """
    Steps:
      1. Wait for Spark DAG to create the combined BigQuery table.
      2. Run `dbt run` for that year.
      3. Run `dbt test`.
      4. Generate dbt docs.
    """

    wait_for_spark_processing = ExternalTaskSensorAsync(
        task_id="wait_for_spark_processing",
        external_dag_id="climate_data_spark_historical_data_processing",
        external_task_id="create_combined_bq_table",
        timeout=10 * 60,
        poke_interval=60,
        deferrable=True,
        check_existence=True,
    )

    dbt_common = {
        "cwd": DBT_PROJECT_DIR,
        "env": {
            # example: "GOOGLE_APPLICATION_CREDENTIALS": "/path/key.json"
        },
    }

    year_template = "{{ params.year }}"

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run --no-write-json "
            f"--vars '{{\"year\": \"{year_template}\"}}'"
        ),
        **dbt_common,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --no-write-json",
        **dbt_common,
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="dbt docs generate",
        **dbt_common,
    )

    wait_for_spark_processing >> dbt_run >> dbt_test >> dbt_docs_generate

dag = climate_data_dbt_transformations()
