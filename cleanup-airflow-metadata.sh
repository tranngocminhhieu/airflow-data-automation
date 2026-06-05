#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AIRFLOW_SERVICE="airflow-scheduler"
RETENTION_DAYS=90

cd "$SCRIPT_DIR"

CLEAN_BEFORE="$(
  python3 - <<PY
from datetime import datetime, timedelta, timezone

retention_days = int("${RETENTION_DAYS}")
clean_before = datetime.now(timezone.utc) - timedelta(days=retention_days)
print(clean_before.strftime("%Y-%m-%dT0:0:0+07:00"))
PY
)"

echo "Cleaning Airflow metadata older than: $CLEAN_BEFORE"

docker compose exec -T "$AIRFLOW_SERVICE" \
  airflow db clean \
    --clean-before-timestamp "$CLEAN_BEFORE" \
    --skip-archive \
    -y

echo "Done."