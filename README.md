# Airflow Data Automation

Apache Airflow (LocalExecutor) for **BI Engineers** who are comfortable with Python + Cron but want
a real orchestrator for monitoring — without changing how they work, and without standing up heavy
infrastructure.

All you need is:
- A Linux server or PC
- Deploy this Airflow on Docker
- Write each DAG to run in **two modes** (CLI and Airflow), if you want to keep running plain Python
- Install extra packages into the External Python Environment when you need them

## Features

1. **A ready-made External Python Environment**\
   Install as many packages as you want without breaking Airflow — no container restart, and it
   persists. Point the `task.external_python` decorator at it via the `EXTERNAL_PYTHON` env var.
2. **Traefik pre-configured (commented out)** to route a domain to Airflow.
3. **Multiple DAG sources pre-configured (commented out):**\
   `LocalDagBundle` for folders on the host, `GitDagBundle` for GitHub repos.
4. **Passing large data between tasks:**
   - **XCom Object Storage** — no need to write intermediate files yourself; Airflow offloads
     automatically when a value is larger than 1 MB. You'll find them under `./data`, and a DAG is
     included to prune them on a schedule.
   - **`./data` (`/opt/airflow/data`)** — where you store data files such as parquet. Reference it
     in a DAG via the `DATA_DIR` env var.
5. **Sample DAGs:**
   - [cleanup-airflow-files.py](dags/cleanup-airflow-files.py) — prunes **data/xcom/** and **logs/**
     on a schedule.
   - [demo.py](dags/demo.py) — two run modes (CLI and Airflow); writes data to files and passes the
     path between tasks.
   - [demo_xcom_objectstorage.py](dags/demo_xcom_objectstorage.py) — two run modes (CLI and
     Airflow); passes large data directly between tasks.
6. **Metadata cleanup:**\
   [cleanup-airflow-metadata.sh](cleanup-airflow-metadata.sh) is included — see the guide below.

## Quick Start

Needs Docker + Docker Compose (production: a Linux server).

```shell
cp .env_example .env              # fill in every REPLACE_... value
docker compose up airflow-init    # first run only: DB + admin user + venv
docker compose up -d
```

Open http://localhost:8080 and log in with the admin account from `.env`.

## Architecture

| Service | Role |
|---|---|
| `postgres` | Metadata database |
| `airflow-apiserver` | Web UI + REST API (host `:8080`) |
| `airflow-scheduler` | Schedules and runs tasks (LocalExecutor runs them here) |
| `airflow-dag-processor` | Parses DAG files |
| `airflow-triggerer` | Runs deferrable operators |
| `airflow-init` | One-shot: DB migrate, admin user, builds the external venv |

## Usage

### Install packages into the External Python Environment

```shell
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv/bin/pip install pandas pyarrow
```

### Create another External Python Environment

```shell
docker compose exec airflow-scheduler python -m venv /opt/airflow/pyenv/venv-special
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv-special/bin/pip install "apache-airflow==${AIRFLOW_VERSION}"
```

### Configure Traefik (optional)

In [.env_example](.env_example), set your domain in `AIRFLOW_HOST`.

In [docker-compose.yaml](docker-compose.yaml):
- Uncomment the `networks` and `labels` blocks under _Reverse proxy (Traefik)_.
- Comment out the `ports` block of `airflow-apiserver`.

### Add DAG sources

#### LocalDagBundle

Set the absolute path of your DAG folder in `DAGS_FOLDER_2` / `DAGS_FOLDER_3` in
[.env_example](.env_example).

#### GitDagBundle

**Step 1** — Create an SSH key:

```shell
ssh-keygen -t ed25519 -C "airflow-git-1" -f ~/.ssh/airflow_git_1 -N ""
chmod 600 ~/.ssh/airflow_git_1
```

**Step 2** — Add the public key to the GitHub repo:\
Repo > Settings > Deploy keys > Add deploy key

**Step 3** — Fill in the `GIT_*` block in [.env_example](.env_example).

### Clean up metadata with cron

Since Airflow 3.0, metadata can no longer be cleaned directly from a DAG, so it has to be done via
bash. [cleanup-airflow-metadata.sh](cleanup-airflow-metadata.sh) is included for this.

Adjust `RETENTION_DAYS` to the number of days you want to keep, then add it to your host's crontab:

```shell
0 0 * * * /path/to/cleanup-airflow-metadata.sh
```

### Check Airflow base requirements

You don't need the External Python Environment if the packages you need already ship with Airflow.

[airflow-base-requirements.txt](airflow-base-requirements.txt) lists the Python libraries bundled with
Airflow 3.2.2. Regenerate it with:

```shell
docker compose exec -T airflow-scheduler \
  python -m pip freeze > airflow-base-requirements.txt
```