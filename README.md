# Airflow Data Automation

Single-server Apache Airflow (LocalExecutor) deployment, pre-tuned for
**BI / Analytics Engineers** — people already comfortable with Python + Cron who now
need a real orchestrator without standing up complex infrastructure.

What makes it different from a vanilla Airflow setup: your pipeline's business logic runs
in a **dedicated Python environment, isolated from Airflow itself**. You can install extra
packages for your pipelines **anytime — no image rebuild, no Airflow restart** — and Airflow
stays stable regardless.

## Highlights

- **Lean deploy**: just Docker, one server, a few commands.
- **1 pipeline = 1 Python file**: runs both as a CLI (`python file.py`) and as an Airflow DAG.
- **Isolated Python env**: business logic runs in its own venv (`/opt/airflow/pyenv/venv`),
  never touching Airflow's environment.
- **`pip install` anytime**: add pipeline packages without rebuild/restart; they persist
  across restarts via a Docker volume.
- **Multiple DAG sources**: beyond the default `./dags`, attach extra host folders or git
  repos (over SSH) as separate DAG bundles — declared entirely in `.env`.

## Architecture

| Service | Role |
|---|---|
| `postgres` | Airflow metadata database |
| `airflow-apiserver` | Web UI + REST API (bound to `127.0.0.1:8080`) |
| `airflow-scheduler` | Schedules and runs tasks (LocalExecutor runs tasks on the scheduler) |
| `airflow-dag-processor` | Parses DAGs from the DAG folders/bundles |
| `airflow-triggerer` | Handles deferrable operators |
| `airflow-init` | Initializes the DB, creates the admin user, builds the business-logic venv |

Business logic runs via `ExternalPythonOperator` / `@task.external_python`, pointed at the
dedicated venv interpreter rather than Airflow's own Python:

```
┌───────────────────────────────┐
│ Airflow (apache/airflow image)│  ← Airflow's own Python, left untouched
│  scheduler / apiserver / ...  │
│                               │
│   @task.external_python  ─────┼──► /opt/airflow/pyenv/venv/bin/python3
└───────────────────────────────┘        ↑ dedicated business-logic venv
                                        (pip install freely, persists on a volume)
```

The full service definition lives in [`docker-compose.yaml`](docker-compose.yaml).

## Requirements

- Docker + Docker Compose
- Production deploys are recommended on a Linux server

## Project layout

```
.
├── docker-compose.yaml      # All service definitions
├── .env_example             # Config template — copy to .env
├── .env                     # Real config (DO NOT commit)
├── .placeholder_ssh_key     # Dummy key, default git-bundle mount when disabled
├── LICENSE
└── dags/                    # Default DAG folder (bundle "dags-folder")
    └── demo.py              # Sample pipeline (CLI + Airflow)
```

## Configuration

All configuration lives in `.env`. Start from the template:

```shell
cp .env_example .env
```

Every variable is documented inline in [`.env_example`](.env_example) — read it there rather
than duplicating the list here. Before deploying, replace **all** `REPLACE_WITH_...` values and
generate a fresh `FERNET_KEY`:

```shell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Deployment

First-time init (DB migration, admin user, business-logic venv):

```shell
docker compose up airflow-init
```

Start the full stack:

```shell
docker compose up -d
```

Open the Web UI at http://127.0.0.1:8080 and log in with the admin account from `.env`.

## Writing a DAG

Each pipeline is a single Python file that runs in two modes. See
[`dags/demo.py`](dags/demo.py) for the full, working sample — the key points:

- **Business logic = self-contained functions.** Every `import` goes *inside* the function,
  and the body must not reference names from the outer scope. Reason: `@task.external_python`
  captures the function's *source* (`inspect.getsource`) and runs it under the venv
  interpreter, so outer-scope names won't exist at execution time.
- **Airflow mode** wraps those same functions with `task.external_python(python=EXTERNAL_PYTHON)`,
  guarded by a `try/except ModuleNotFoundError` so the file still runs as a plain CLI script.

Quick logic test without Airflow:

```shell
python dags/demo.py
```

## Managing the Python environment

A default venv is created at `/opt/airflow/pyenv/venv` with `apache-airflow` matching the
image version. It lives on a Docker volume, so it **persists across restarts**.

Install extra packages (takes effect on the next task run — no restart, no rebuild):

```shell
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv/bin/pip install <package>
```

Need an isolated venv for a special pipeline? Create it under `/opt/airflow/pyenv/` (so it sits
on the persistent volume), install `apache-airflow` at the same `AIRFLOW_VERSION`, then point
the DAG's `EXTERNAL_PYTHON` at the new interpreter:

```shell
docker compose exec airflow-scheduler python -m venv /opt/airflow/pyenv/venv-special
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv-special/bin/pip install --upgrade pip
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv-special/bin/pip install "apache-airflow==${AIRFLOW_VERSION}"
```

```python
EXTERNAL_PYTHON = "/opt/airflow/pyenv/venv-special/bin/python3"
```

## DAG sources (bundles)

The default DAG folder is `./dags` (bundle `dags-folder`) — no config needed.

When DAGs live elsewhere, each source is registered as its own **DAG bundle** (an Airflow 3
feature) with a distinct name, shown separately in the Web UI's *Bundle* column. Everything is
driven from `.env`; the wiring is in
[`AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST`](docker-compose.yaml).

### Extra host folders

Point the optional `DAGS_FOLDER_2` / `DAGS_FOLDER_3` variables (see [`.env_example`](.env_example))
at folders on the host. Each is mounted **outside** `/opt/airflow/dags` (so the primary bundle
doesn't scan it twice) and registered as bundle `dags-folder-2` / `dags-folder-3`. Unused slots
default to an auto-created empty dir (harmless). Need more than two? Add a mount and a matching
list entry in `docker-compose.yaml`.

> ⚠️ `dag_id` must be **unique across all bundles** — duplicates cause import errors.

> Note (macOS): if a folder lives outside Docker Desktop's shared paths (e.g. outside `/Users`),
> add it under **Docker Desktop → Settings → Resources → File Sharing**.

### Git bundles (SSH)

A bundle can pull DAGs **directly from a git remote** (`GitDagBundle`) — no mount; Airflow
clones and pulls the tracked branch every `refresh_interval`. This is the most production-grade
option: devs push to git, Airflow updates itself.

**Create a deploy key.** Airflow needs an SSH private key to clone. Generate a dedicated key
pair (no passphrase, so Airflow can use it unattended):

```shell
ssh-keygen -t ed25519 -C "airflow-git-1" -f ~/.ssh/airflow_git_1 -N ""
chmod 600 ~/.ssh/airflow_git_1
```

- `~/.ssh/airflow_git_1` — **private key** → point `GIT_SSH_KEY_1` at it.
- `~/.ssh/airflow_git_1.pub` — **public key** → register on the remote as a read-only deploy key
  (GitHub: *Settings → Deploy keys*; GitLab: *Settings → Repository → Deploy keys*).

Each slot should use its own deploy key (GitHub deploy keys are per-repo). The setup uses
`strict_host_key_checking: no`, so no `known_hosts` entry is needed.

**Enable a slot.** Two independent slots (`git-1`, `git-2`) ship **disabled by default** (no
errors when off). Uncomment and fill the `GIT_*_n` block in [`.env_example`](.env_example) →
`.env`. A bundle is added only when its `GIT_REPO_URL_n` is set (via Compose's `${VAR:+...}`),
so leaving a slot blank means no bundle and no error. Then recreate:

```shell
docker compose up -d
```

Need more than two repos? Add a `git-3` slot following the same pattern (a `${GIT_REPO_URL_3:+...}`
list entry, an `AIRFLOW_CONN_GIT_3` connection, and a key mount) in `docker-compose.yaml`.

## Operations

```shell
# List DAGs (shows which bundle each belongs to)
docker compose exec airflow-scheduler airflow dags list

# Check DAG import errors
docker compose exec airflow-scheduler airflow dags list-import-errors

# Trigger a DAG manually
docker compose exec airflow-scheduler airflow dags trigger <dag_id>

# Tail logs
docker compose logs -f airflow-scheduler

# Stop the stack
docker compose down

# Stop and drop volumes (loses the venv + metadata DB)
docker compose down -v
```