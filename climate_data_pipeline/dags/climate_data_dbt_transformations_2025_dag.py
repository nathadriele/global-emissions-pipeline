from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

# ---------------------------------------------------------------------------
# Global settings
# ---------------------------------------------------------------------------

LOCAL_TZ = pendulum.timezone("America/Sao_Paulo")

DEFAULT_ARGS: dict[str, object] = {
    "owner": "zoomcamp",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_PROJECT_DIR = os.path.join(
    AIRFLOW_HOME,
    "dbt_climate_data",
    "climate_transforms",
)

# Processing year now defaults to 2025
PROCESSING_YEAR: str = Variable.get("processing_year", default_var="2025")

# ---------------------------------------------------------------------------
# DAG definition (TaskFlow API)
# ---------------------------------------------------------------------------

@dag(
    dag_id="climate_data_dbt_transformations",
    description="Run dbt transformations on combined climate data",
    default_args=DEFAULT_ARGS,
    schedule_interval=None, 
    start_date=pendulum.datetime(2023, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["climate_data", "dbt", "bigquery"],
)
def climate_data_dbt_transformations():
    """
    Pipeline steps (default year = 2025 unless overridden via Variable or dagrun.conf):
      1. Wait for the Spark DAG to create the consolidated BigQuery table.
      2. Run `dbt run`, passing the year via --vars.
      3. Run `dbt test` to validate the models.
      4. Generate dbt documentation artifacts.
    """

    wait_for_spark_processing = ExternalTaskSensor(
        task_id="wait_for_spark_processing",
        external_dag_id="climate_data_spark_historical_data_processing",
        external_task_id="create_combined_bq_table",
        timeout=10 * 60,
        poke_interval=60,
        mode="poke",
        soft_fail=False,
        check_existence=True,
    )

    dbt_env = {
        # Sensitive vars go into your secrets back‑end
    }

    dbt_common_kwargs = {
        "cwd": DBT_PROJECT_DIR,
        "env": dbt_env,
    }

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run --no-write-json "
            f"--vars '{{\"year\": \"{PROCESSING_YEAR}\"}}'"
        ),
        **dbt_common_kwargs,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --no-write-json",
        **dbt_common_kwargs,
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="dbt docs generate",
        **dbt_common_kwargs,
    )

    wait_for_spark_processing >> dbt_run >> dbt_test >> dbt_docs_generate

dag = climate_data_dbt_transformations()
