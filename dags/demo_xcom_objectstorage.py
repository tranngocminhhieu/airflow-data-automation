import os
from datetime import datetime, timedelta


# === Business logic ===
# Tasks return/accept pandas DataFrames directly. The Object Storage XCom backend
# (configured in docker-compose.yaml) offloads any XCom larger than the threshold to
# /opt/airflow/data/xcom instead of the metadata DB -- so no manual parquet paths.
# pandas + pyarrow must be installed in BOTH:
#   - the Airflow base env (compose: _PIP_ADDITIONAL_REQUIREMENTS) -> to (de)serialize the frame
#   - the business-logic venv -> so the task code runs:
#     docker compose exec airflow-scheduler /opt/airflow/pyenv/venv/bin/pip install pandas pyarrow

def extract_customers():
    import numpy as np
    import pandas as pd

    # ~100k rows x 20 cols of synthetic data -> well over the 1 MiB threshold, so it offloads.
    n = 100_000
    rng = np.random.default_rng(42)
    df = pd.DataFrame({f"metric_{i}": rng.random(n) for i in range(18)})
    df.insert(0, "id", np.arange(n))
    df["spend"] = rng.uniform(0, 500, n)
    df.loc[rng.choice(n, n // 10, replace=False), "spend"] = None  # ~10% missing
    print(f"Extracted {len(df)} rows x {df.shape[1]} cols")
    return df


def clean_customers(df):
    df = df.copy()
    df["spend"] = df["spend"].fillna(0.0)
    df = df.drop_duplicates(subset=["id"])
    print(f"Cleaned -> {len(df)} rows")
    return df


def load_customers(df):
    print(f"Loaded {len(df)} customers, total spend = {df['spend'].sum():,.2f}")


def main():
    load_customers(clean_customers(extract_customers()))


if __name__ == "__main__":
    main()


# === Airflow mode ===
else:
    from functools import partial
    from airflow.sdk import dag, task

    task_extpy = partial(task.external_python, python=os.getenv("EXTERNAL_PYTHON"))

    @dag(
        dag_id="demo_pipeline_xcom_storage",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["customer"],
        default_args={"owner": "Airflow", "retries": 1, "retry_delay": timedelta(minutes=1)}
    )
    def customer_pipeline_xcom_storage():
        extract = task_extpy()(extract_customers)
        clean = task_extpy()(clean_customers)
        load = task_extpy()(load_customers)

        load(clean(extract()))

    customer_pipeline_xcom_storage()
