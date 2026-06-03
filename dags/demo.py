from __future__ import annotations

import os
from datetime import datetime

# Dedicated venv for business logic, on a persistent volume inside the container.
# Isolated from Airflow's own env, so installing extra packages never affects
# Airflow and needs no restart/rebuild:
#   docker compose exec airflow-scheduler /opt/airflow/pyenv/venv/bin/pip install <package>
# Path comes from EXTERNAL_PYTHON in .env (with a fallback default).
EXTERNAL_PYTHON = os.getenv("EXTERNAL_PYTHON", "/opt/airflow/pyenv/venv/bin/python3")


# =========================================================
# Business logic (CLI mode: plain Python)
# =========================================================

def extract_customers():
    import pandas as pd
    import requests
    print("Extract customers")
    # real code here


def clean_customers():
    import pandas as pd

    print("Clean customers")
    # real code here


def load_customers():
    import sqlalchemy

    print("Load customers")
    # real code here


def main():
    extract_customers()
    clean_customers()
    load_customers()


# =========================================================
# Airflow mode: TaskFlow API + @task.external_python
# Reuses the same separated business-logic functions above: wrap them with
# task.external_python so they run under the EXTERNAL_PYTHON venv interpreter.
# The decorator captures each function's source (inspect.getsource); since the
# functions are self-contained (imports inside), they run fine in the venv.
# =========================================================

try:
    from airflow.sdk import dag, task

    @dag(
        dag_id="customer_pipeline_taskapi",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["customer"],
    )
    def customer_pipeline_taskapi():
        extract = task.external_python(python=EXTERNAL_PYTHON)(extract_customers)
        clean = task.external_python(python=EXTERNAL_PYTHON)(clean_customers)
        load = task.external_python(python=EXTERNAL_PYTHON)(load_customers)

        extract() >> clean() >> load()

    customer_pipeline_taskapi()
except ModuleNotFoundError:
    pass


if __name__ == "__main__":
    main()