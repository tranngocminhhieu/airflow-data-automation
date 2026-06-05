# Airflow Data Automation

Single-server Apache Airflow (LocalExecutor) for **BI / Analytics Engineers** who already write
Python + Cron jobs and want a real orchestrator without heavy infrastructure.

The core idea: your pipeline logic runs in a **dedicated Python venv, isolated from Airflow**. You
`pip install` whatever your pipelines need — **anytime, no image rebuild, no Airflow restart** —
and Airflow itself stays untouched and stable.

## Features

- **1 pipeline = 1 Python file** that runs two ways: `python file.py` for a local test, and as a
  scheduled Airflow DAG — same code, same steps.
- **Isolated venv** for business logic; `pip install` persists on a volume, takes effect on the
  next run.
- **Large data between tasks** via parquet files in `./data` (not XCom).
- **Many DAG sources**: the default `./dags`, extra host folders, or git repos over SSH — each a
  separate bundle, all configured in `.env`.
- **Optional public domain** with HTTPS through an existing Traefik proxy.

## Architecture

| Service | Role |
|---|---|
| `postgres` | Metadata database |
| `airflow-apiserver` | Web UI + REST API (host `:8080`) |
| `airflow-scheduler` | Schedules and runs tasks (LocalExecutor runs them here) |
| `airflow-dag-processor` | Parses DAG files |
| `airflow-triggerer` | Runs deferrable operators |
| `airflow-init` | One-shot: DB migrate, admin user, builds the business-logic venv |

Pipeline code runs through `@task.external_python`, pointed at the venv interpreter — never
Airflow's own Python:

```
┌───────────────────────────────┐
│ Airflow (apache/airflow image)│  ← left untouched
│   @task.external_python  ─────┼──► /opt/airflow/pyenv/venv/bin/python3
└───────────────────────────────┘        ↑ pip install freely, persists on a volume
```

## Quick start

Needs Docker + Docker Compose (production: a Linux server).

```shell
cp .env_example .env     # fill in every REPLACE_WITH_... value
docker compose up airflow-init   # first run only: DB + admin user + venv
docker compose up -d
```

Open http://localhost:8080 and log in with the admin account from `.env`.

## Configuration

Everything lives in `.env`; each variable is documented inline in
[`.env_example`](.env_example). What you must set:

- **Secrets** — `FERNET_KEY` (generate below), `AIRFLOW__API_AUTH__JWT_SECRET`, Postgres
  credentials, and the web admin user/password.
- **Tuning** (optional) — `AIRFLOW__CORE__PARALLELISM` and the `MAX_ACTIVE_*` limits.

```shell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Writing a DAG

A pipeline is one Python file (see [`dags/demo.py`](dags/demo.py)) built from **self-contained
functions**: every `import` goes *inside* the function and the body uses no outer-scope names.
That's required — `@task.external_python` ships the function's *source* to the venv interpreter,
so anything defined outside it won't exist at run time.

The same file runs two ways, split by an `__name__` guard:

```python
if __name__ == "__main__":
    main()                      # `python dags/demo.py` — local test, no Airflow
else:                           # Airflow imports the file as a module → register the DAG here
    extract = task.external_python(python=EXTERNAL_PYTHON)(extract_customers)
    # ... wire the steps into a @dag
```

So you develop and test the logic locally, then drop the file in `dags/` and Airflow schedules it
with the **same steps** — nothing to rewrite. `EXTERNAL_PYTHON` is provided by the stack
(`docker-compose.yaml`), pointing at the venv.

### Passing large data between tasks

In plain Python `load(clean(extract()))` just hands over an in-memory object. In Airflow each task
is a separate process, so data must be serialized — and **plain XCom goes through the metadata DB,
for small values only**, never a 100K-row frame. There are two ways to handle big data here:

**1. Parquet path** — [`dags/demo.py`](dags/demo.py). Each step writes its output as parquet under
`./data` and returns just the **file path**; the next step reads it. Only the path travels through
XCom; the data stays on disk. Explicit, and the base Airflow env stays lean.

`./data` is the same place everywhere: `/opt/airflow/data` in the container is a host bind mount,
so the files also appear in the repo's `./data`, and a local `python dags/demo.py` writes there
too. Open or delete the parquet anytime.

**2. Object Storage XCom backend** — [`dags/demo_xcom_storage.py`](dags/demo_xcom_storage.py).
Tasks `return df` and accept `df` directly (the ergonomic `load(clean(extract()))` style); the
backend transparently offloads any XCom **larger than the threshold** to `./data/xcom` and keeps
small values in the DB. Configured globally in `docker-compose.yaml`
(`AIRFLOW__CORE__XCOM_BACKEND` + `AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_*`: path, `THRESHOLD`
1 MiB, `gzip`). Airflow re-materializes the returned frame in its own process before offloading it,
so the base env also needs pandas + pyarrow — the `apache/airflow` image already ships both, so no
extra install is required.

Use **1** when you want the base image lean; use **2** when you'd rather pass DataFrames directly
and let the backend manage spillover. Both keep the **same 3 steps as the local run**.

> The backend only deletes an offloaded file when its XCom row is deleted (task re-run, run/TI
> deleted) — it never expires files by age. [`dags/cleanup_xcom.py`](dags/cleanup_xcom.py)
> (`maintenance_cleanup`) is a `@daily` DAG that handles the things Airflow grows but never
> auto-cleans: it prunes `./data/xcom` and `./logs` by age and runs `airflow db clean` on old
> metadata. Retention is set by the `*_RETENTION_DAYS` constants at the top of the file.

> Works because LocalExecutor runs all of a run's tasks in one container. With multiple workers,
> use shared storage (NFS / object storage); if runs overlap, namespace the path per run.

## Python environment

The venv at `/opt/airflow/pyenv/venv` ships with only `apache-airflow`. Install what your
pipelines need; it persists across restarts and applies on the next task run:

```shell
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv/bin/pip install pandas pyarrow
```

Need a different dependency set for one pipeline? Make a second venv next to it and point that
DAG's `EXTERNAL_PYTHON` at the new interpreter:

```shell
docker compose exec airflow-scheduler python -m venv /opt/airflow/pyenv/venv-special
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv-special/bin/pip install "apache-airflow==${AIRFLOW_VERSION}"
```

## DAG sources (bundles)

The default folder is `./dags` (bundle `dags-folder`). Each extra source is registered as its own
named **bundle** (Airflow 3), shown separately in the UI — all configured in `.env`.

> ⚠️ `dag_id` must be **unique across all bundles**.

### Extra host folders

Set `DAGS_FOLDER_2` / `DAGS_FOLDER_3` in `.env` → bundles `dags-folder-2` / `-3`. Unused slots are
harmless.

> macOS: a folder outside Docker Desktop's shared paths must be added under
> **Settings → Resources → File Sharing**.

### Git repos (SSH)

A bundle can pull DAGs straight from a git remote — Airflow clones and re-pulls on a schedule. Two
slots (`git-1`, `git-2`), off by default. Create a read-only deploy key:

```shell
ssh-keygen -t ed25519 -C "airflow-git-1" -f ~/.ssh/airflow_git_1 -N ""
chmod 600 ~/.ssh/airflow_git_1
```

Point `GIT_SSH_KEY_1` at the private key, add the `.pub` as a deploy key on GitHub/GitLab, then
fill the `GIT_*_1` block in `.env` and `docker compose up -d`. A slot activates only when its
`GIT_REPO_URL_n` is set, so blank slots cause no error.

## Public domain via Traefik (optional)

Off by default. To serve the UI on a public domain with HTTPS through an existing
[Traefik](https://traefik.io/) proxy:

```shell
docker network create proxy_net    # the shared network your Traefik is on
# in .env:  AIRFLOW_HOST=airflow.example.com
```

Then in `docker-compose.yaml` on `airflow-apiserver`: comment out the `ports` block and uncomment
the `networks` + `labels` block and the `proxy_net` network at the bottom — then
`docker compose up -d`. The apiserver stays on `default` (to reach postgres + scheduler) and also
joins `proxy_net` (for Traefik).

> The labels assume network `proxy_net`, entrypoint `websecure`, resolver `letsencrypt` — change
> them to match your Traefik. Point the domain's DNS at the host; the cert issues automatically.

## Operations

```shell
docker compose exec airflow-scheduler airflow dags list                 # list DAGs + bundles
docker compose exec airflow-scheduler airflow dags list-import-errors   # find parse errors
docker compose exec airflow-scheduler airflow dags trigger <dag_id>     # run now
docker compose logs -f airflow-scheduler                                # tail logs
docker compose down        # stop
docker compose down -v     # stop + wipe volumes (loses the venv + metadata DB)
```