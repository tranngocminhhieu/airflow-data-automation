# Airflow Data Automation

Single-server Apache Airflow (LocalExecutor) for **BI / Analytics Engineers** who know
Python + Cron and want a real orchestrator without heavy infrastructure.

The twist: pipeline business logic runs in a **dedicated Python venv, isolated from Airflow**.
Install pipeline packages anytime — **no image rebuild, no Airflow restart** — while Airflow
stays stable.

## Highlights

- **Lean**: Docker, one server, a few commands.
- **1 pipeline = 1 Python file**: runs as a CLI (`python file.py`) and as an Airflow DAG.
- **Isolated venv** (`/opt/airflow/pyenv/venv`): never touches Airflow's own env.
- **`pip install` anytime**: no rebuild/restart; packages persist on a Docker volume.
- **Multiple DAG sources**: extra host folders or git repos (SSH) as separate bundles, via `.env`.

## Architecture

| Service | Role |
|---|---|
| `postgres` | Metadata database |
| `airflow-apiserver` | Web UI + REST API (`127.0.0.1:8080`) |
| `airflow-scheduler` | Schedules + runs tasks (LocalExecutor runs them here) |
| `airflow-dag-processor` | Parses DAGs |
| `airflow-triggerer` | Deferrable operators |
| `airflow-init` | DB migrate, admin user, builds the business-logic venv |

Business logic runs via `@task.external_python`, pointed at the venv interpreter instead of
Airflow's Python:

```
┌───────────────────────────────┐
│ Airflow (apache/airflow image)│  ← left untouched
│   @task.external_python  ─────┼──► /opt/airflow/pyenv/venv/bin/python3
└───────────────────────────────┘        ↑ pip install freely, persists on a volume
```

Full definition: [`docker-compose.yaml`](docker-compose.yaml).

## Quick start

Requires Docker + Docker Compose (production: Linux server).

```shell
cp .env_example .env          # then fill in REPLACE_WITH_... values
docker compose up airflow-init  # first-time init
docker compose up -d
```

Open http://127.0.0.1:8080 and log in with the admin account from `.env`.

Generate a fresh `FERNET_KEY` before deploying:

```shell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Layout

```
.
├── docker-compose.yaml   # All services
├── .env_example          # Config template → copy to .env
├── .placeholder_ssh_key  # Default git-bundle key mount when disabled
└── dags/demo.py          # Sample pipeline (CLI + Airflow)
```

Every config var is documented inline in [`.env_example`](.env_example).

## Writing a DAG

One Python file, two modes — see [`dags/demo.py`](dags/demo.py):

- **Business logic = self-contained functions**: all `import`s *inside* the function, no
  outer-scope references. `@task.external_python` runs the function's *source*
  (`inspect.getsource`) under the venv interpreter, where outer names don't exist.
- **Airflow mode** wraps those functions with `task.external_python(python=EXTERNAL_PYTHON)`,
  inside `try/except ModuleNotFoundError` so the file still runs as a plain CLI script.

```shell
python dags/demo.py   # quick logic test, no Airflow
```

### Large data between tasks

In plain Python, `load(clean(extract()))` just passes an in-memory reference — free. In Airflow,
each task is a separate process, so data between them must be serialized. The default channel is
**XCom (the metadata DB) — for small metadata only**, never a 100K-row frame.

Pattern (see `demo.py`): each step writes its output as **parquet** under `./data` and returns
only the **file path**; the next step reads it. Only the small path travels through XCom; the data
stays on disk. The DAG has the **same 3 steps as the CLI** (`extract → clean → load`) — no extra
orchestration tasks.

The working dir resolves to `./data` everywhere: `/opt/airflow/data` inside the container is a
host bind mount, so the files also show up in the repo's `./data` — and a host run
(`python dags/demo.py`) writes the same `./data`. Inspect (or delete) the parquet anytime.

> This relies on LocalExecutor (all tasks of a run share one filesystem). With multiple workers,
> point the working dir at shared storage (NFS / object storage) instead. If runs can overlap,
> namespace the path (e.g. per run) so they don't clobber each other.

## Python environment

A venv with `apache-airflow` (matching the image) is prebuilt at `/opt/airflow/pyenv/venv` on a
volume, so it persists across restarts. Add packages (effective next task run):

```shell
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv/bin/pip install <package>
```

Separate venv for a special pipeline — create under `/opt/airflow/pyenv/`, install
`apache-airflow` at the same version, then point the DAG's `EXTERNAL_PYTHON` at it:

```shell
docker compose exec airflow-scheduler python -m venv /opt/airflow/pyenv/venv-special
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv-special/bin/pip install "apache-airflow==${AIRFLOW_VERSION}"
```

## DAG sources (bundles)

Default folder is `./dags` (bundle `dags-folder`). Other sources become their own named
**DAG bundle** (Airflow 3), shown separately in the UI. All driven from `.env`; wiring in
[`AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST`](docker-compose.yaml).

> ⚠️ `dag_id` must be **unique across all bundles**.

### Extra host folders

Set `DAGS_FOLDER_2` / `DAGS_FOLDER_3` in `.env` → bundles `dags-folder-2` / `-3`. Each mounts
outside `/opt/airflow/dags` so the primary bundle doesn't double-scan; unused slots are harmless.

> macOS: folders outside Docker Desktop's shared paths need adding under
> **Settings → Resources → File Sharing**.

### Git bundles (SSH)

A bundle can pull DAGs straight from a git remote (`GitDagBundle`) — Airflow clones and pulls
every `refresh_interval`. Two slots (`git-1`, `git-2`), off by default.

```shell
# 1. Deploy key (no passphrase)
ssh-keygen -t ed25519 -C "airflow-git-1" -f ~/.ssh/airflow_git_1 -N ""
chmod 600 ~/.ssh/airflow_git_1
```

- private key `~/.ssh/airflow_git_1` → `GIT_SSH_KEY_1`
- public key `.pub` → repo's read-only **deploy key** (GitHub/GitLab)

Then uncomment + fill the `GIT_*_1` block in `.env` and `docker compose up -d`. A bundle is added
only when `GIT_REPO_URL_n` is set, so blank slots cause no error. `strict_host_key_checking: no`
is set, so no `known_hosts` needed.

## Public domain via Traefik (optional)

Off by default (UI on `127.0.0.1:8080` only). To serve a public domain with HTTPS via an existing
[Traefik](https://traefik.io/) reverse proxy:

```shell
docker network create proxy_net   # shared network your Traefik is on
# .env:  AIRFLOW_HOST=airflow.example.com
```

In `docker-compose.yaml`, on `airflow-apiserver`: **comment out `ports`**, **uncomment** the
`networks` + `labels` block and the `proxy_net` network at the bottom — then `docker compose up -d`.

The apiserver joins both `default` (to reach postgres + scheduler) and `proxy_net` (for Traefik).
Labels assume network `proxy_net`, entrypoint `websecure`, resolver `letsencrypt` — match them to
your Traefik's config (mismatched names are the usual cause of 404s). Point DNS at the host;
Traefik issues the cert once `:80`/`:443` are reachable.

## Operations

```shell
docker compose exec airflow-scheduler airflow dags list                 # list DAGs + bundles
docker compose exec airflow-scheduler airflow dags list-import-errors   # import errors
docker compose exec airflow-scheduler airflow dags trigger <dag_id>     # manual trigger
docker compose logs -f airflow-scheduler                                # tail logs
docker compose down        # stop
docker compose down -v     # stop + drop volumes (loses venv + metadata DB)
```