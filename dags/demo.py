from __future__ import annotations

import os
from datetime import datetime

# Venv riêng cho business logic, nằm trên volume persist trong container.
# Tách biệt env của Airflow nên cài thêm package không ảnh hưởng Airflow,
# không cần restart/rebuild:
#   docker compose exec airflow-scheduler /opt/airflow/pyenv/venv/bin/pip install <package>
# Path lấy từ biến EXTERNAL_PYTHON trong .env (có default phòng khi thiếu).
EXTERNAL_PYTHON = os.getenv("EXTERNAL_PYTHON", "/opt/airflow/pyenv/venv/bin/python3")


# =========================================================
# Business logic (CLI mode: chạy thuần Python)
# =========================================================

def extract_customers():
    import pandas as pd
    import requests
    print("Extract customers")
    # code thật ở đây


def clean_customers():
    import pandas as pd

    print("Clean customers")
    # code thật ở đây


def load_customers():
    import sqlalchemy

    print("Load customers")
    # code thật ở đây


def main():
    extract_customers()
    clean_customers()
    load_customers()


# =========================================================
# Airflow mode: TaskFlow API + @task.external_python
# Tái sử dụng đúng các hàm business logic tách riêng ở trên: bọc chúng
# bằng task.external_python để chạy bằng interpreter của venv EXTERNAL_PYTHON.
# Decorator lấy source của hàm (inspect.getsource), mà các hàm đã
# self-contained (import nằm bên trong) nên chạy trong venv được.
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