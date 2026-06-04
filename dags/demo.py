from __future__ import annotations

import os
from datetime import datetime, timedelta

# Interpreter of the business-logic venv (set in docker-compose.yaml), used by external_python.
EXTERNAL_PYTHON = os.getenv("EXTERNAL_PYTHON", "/opt/airflow/pyenv/venv/bin/python3")

# Working dir for large data passed between tasks: steps exchange a parquet PATH via XCom
# (small), never the data itself. Resolved relative to this file so it's always ./data —
# /opt/airflow/data in the container (a host bind mount) == the repo's ./data on the host.
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


# === Business logic ===
# Self-contained (imports inside) so external_python can run the source under the venv.
# pandas + pyarrow aren't in the default venv; install once:
#   docker compose exec airflow-scheduler /opt/airflow/pyenv/venv/bin/pip install pandas pyarrow

def extract_customers(workdir):
    import pandas as pd

    df = pd.DataFrame(
        [
            {"id": 1, "name": " alice ", "email": "ALICE@example.com", "spend": 120.5},
            {"id": 2, "name": "bob", "email": "bob@example.com", "spend": None},
            {"id": 3, "name": " carol", "email": None, "spend": 80.0},
        ]
    )
    path = f"{workdir}/customers_raw.parquet"
    df.to_parquet(path)
    print(f"Extracted {len(df)} rows -> {path}")
    return path


def clean_customers(in_path):
    import pandas as pd

    df = pd.read_parquet(in_path)
    df["name"] = df["name"].str.strip().str.title()
    df["spend"] = df["spend"].fillna(0.0)
    df = df.dropna(subset=["email"])  # drop customers without an email
    out_path = in_path.replace("_raw", "_clean")
    df.to_parquet(out_path)
    print(f"Cleaned -> {len(df)} rows -> {out_path}")
    return out_path


def load_customers(in_path):
    import pandas as pd

    df = pd.read_parquet(in_path)
    print(f"Loaded {len(df)} customers, total spend = {df['spend'].sum()}")


def main():
    # CLI run: stage in ./data and keep the files so you can open the parquet output.
    load_customers(clean_customers(extract_customers(DATA_DIR)))


# === Airflow mode ===
# __name__ guard: `python demo.py` runs CLI only and never builds the DAG (even with Airflow
# installed). Airflow imports the file as a module, which is when the DAG registers.
# Same 3 steps as main(), wrapped to run under the venv; only parquet PATHS flow through XCom.

if __name__ == "__main__":
    main()
else:
    from airflow.sdk import dag, task

    default_args = {
        "owner": "Airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    }

    @dag(
        dag_id="demo_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["customer"],
        default_args=default_args,
    )
    def customer_pipeline_taskapi():
        extract = task.external_python(python=EXTERNAL_PYTHON)(extract_customers)
        clean = task.external_python(python=EXTERNAL_PYTHON)(clean_customers)
        load = task.external_python(python=EXTERNAL_PYTHON)(load_customers)

        load(clean(extract(DATA_DIR)))  # same shape as main()

    customer_pipeline_taskapi()