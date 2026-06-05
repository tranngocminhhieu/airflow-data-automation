import os
from datetime import datetime, timedelta


# === Business logic ===
# pandas + pyarrow aren't in the default venv; install once:
# docker compose exec airflow-scheduler /opt/airflow/pyenv/venv/bin/pip install pandas pyarrow

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
    from airflow.sdk import dag, task

    EXTERNAL_PYTHON = os.getenv("EXTERNAL_PYTHON")


    @dag(
        dag_id="demo_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["customer"],
        default_args={"owner": "Airflow", "retries": 1, "retry_delay": timedelta(minutes=1)}
    )
    def customer_pipeline_taskapi():
        extract = task.external_python(python=EXTERNAL_PYTHON)(extract_customers)
        clean = task.external_python(python=EXTERNAL_PYTHON)(clean_customers)
        load = task.external_python(python=EXTERNAL_PYTHON)(load_customers)

        load(clean(extract('/opt/airflow/data')))

    customer_pipeline_taskapi()