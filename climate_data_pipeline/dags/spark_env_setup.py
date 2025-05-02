"""
spark_env_setup.py
Purpose: Configure Java and Spark environment variables for use inside
         Airflow tasks/operators that invoke PySpark or spark‑submit.

How to customise:
  • In the Airflow UI → Admin → Variables, create (or update) the keys below:
      - java_home
      - spark_home
      - airflow_python     (optional; defaults to sys.executable)
"""

import os
import sys
from airflow.models import Variable
from pathlib import Path

# ---------------------------------------------------------------------------
# Helper to load an Airflow Variable with a default
# ---------------------------------------------------------------------------
def _var(key: str, default: str | None = None) -> str:
    return Variable.get(key, default_var=default).rstrip("/")

# ---------------------------------------------------------------------------
# Resolve paths (fall back to current Python executable where appropriate)
# ---------------------------------------------------------------------------
JAVA_HOME   = _var("java_home",   "/opt/java")
SPARK_HOME  = _var("spark_home",  "/opt/spark")
PYTHON_BIN  = _var("airflow_python", sys.executable)

# ---------------------------------------------------------------------------
# Export environment variables
# ---------------------------------------------------------------------------
os.environ.update(
    {
        "JAVA_HOME": JAVA_HOME,
        "SPARK_HOME": SPARK_HOME,
        "PYSPARK_PYTHON": PYTHON_BIN,
        "PYSPARK_DRIVER_PYTHON": PYTHON_BIN,
    }
)

spark_bin = str(Path(SPARK_HOME) / "bin")
current_path = os.environ.get("PATH", "")

if spark_bin not in current_path.split(os.pathsep):
    os.environ["PATH"] = os.pathsep.join([spark_bin, current_path])
