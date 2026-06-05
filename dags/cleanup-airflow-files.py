import os
import time
from datetime import datetime, timedelta

from airflow.sdk import dag, task

XCOM_RETENTION_DAYS = 3
LOG_RETENTION_DAYS = 30

def _prune_dir(base, retention_days):
    """Delete files older than retention_days under base, drop emptied subdirs, return a summary."""
    if not os.path.isdir(base):
        return f"{base} does not exist, skipped"

    cutoff = time.time() - retention_days * 86400
    removed, freed = 0, 0
    for root, _dirs, files in os.walk(base):
        for name in files:
            fp = os.path.join(root, name)
            try:
                if os.path.getmtime(fp) < cutoff:
                    freed += os.path.getsize(fp)
                    os.remove(fp)
                    removed += 1
            except FileNotFoundError:
                pass  # raced with another deleter -- already gone

    for root, _dirs, _files in os.walk(base, topdown=False):
        if root != base and not os.listdir(root):
            try:
                os.rmdir(root)
            except OSError:
                pass

    return f"Pruned {removed} files (~{freed / 1e6:.1f} MB) older than {retention_days}d from {base}"


@dag(
    dag_id="maintenance_cleanup",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["maintenance", "demo"],
    default_args={"owner": "Airflow", "retries": 1, "retry_delay": timedelta(minutes=1)},
)
def maintenance_cleanup():
    @task
    def prune_xcom():
        path = os.getenv("AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH", "file:///opt/airflow/data/xcom")
        print(_prune_dir(path.split("://", 1)[-1], XCOM_RETENTION_DAYS))

    @task
    def prune_logs():
        base = os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs")
        print(_prune_dir(base, LOG_RETENTION_DAYS))

    prune_xcom()
    prune_logs()

maintenance_cleanup()
