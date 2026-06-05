import os
from datetime import datetime, timedelta


# === Business logic (CLI mode) ===
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
    load_customers(clean_customers(extract_customers('../data')))


if __name__ == "__main__":
    main()


# === Airflow mode ===
else:
    from functools import partial
    from airflow.sdk import dag, task

    task_extpy = partial(task.external_python, python=os.getenv("EXTERNAL_PYTHON"))

    @dag(
        dag_id="demo_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["demo"],
        default_args={"owner": "Airflow", "retries": 1, "retry_delay": timedelta(minutes=1)}
    )
    def customer_pipeline_taskapi():
        extract = task(extract_customers)     # Airflow Environment: pandas, pyarrow included
        clean = task_extpy()(clean_customers) # External Python Environment: need to install pandas, pyarrow
        load = task_extpy()(load_customers)   # External Python Environment: need to install pandas, pyarrow

        load(clean(extract(os.getenv('DATA_DIR')))) # /opt/airflow/data

    customer_pipeline_taskapi()