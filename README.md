# Airflow Data Automation

Deploy Apache Airflow (LocalExecutor) trên một server duy nhất, tùy chỉnh sẵn cho
**BI Engineer / Analytics Engineer** — những người đã quen Python + Cron và giờ cần một
trình điều phối (orchestrator) đúng nghĩa nhưng không muốn dựng hạ tầng phức tạp.

Điểm khác biệt so với cách dùng Airflow thông thường: business logic của pipeline chạy
trong một **Python environment riêng, tách biệt với Airflow**. Nhờ vậy bạn có thể cài thêm
package cho pipeline bất cứ lúc nào mà **không cần build lại image, không cần restart Airflow**,
và bản thân Airflow luôn ổn định.

## Tính năng chính

- **Deploy gọn**: chỉ cần Docker, một server, vài lệnh.
- **1 pipeline = 1 file Python**: chạy được cả bằng CLI (`python file.py`) lẫn Airflow.
- **Python env tách biệt**: business logic chạy trong venv riêng (`/opt/airflow/pyenv/venv`),
  không đụng tới env của Airflow.
- **`pip install` bất cứ lúc nào**: cài thêm package cho pipeline mà không rebuild / restart;
  package persist qua restart nhờ Docker volume.
- **Nhiều nguồn DAG**: ngoài thư mục `./dags` mặc định, gắn thêm thư mục khác trên host hoặc git
  repo (qua SSH) làm DAG bundle riêng — chỉ cần khai báo trong `.env`.

## Kiến trúc

| Thành phần | Vai trò |
|---|---|
| `postgres` | Metadata database của Airflow |
| `airflow-apiserver` | Web UI + REST API (cổng `127.0.0.1:8080`) |
| `airflow-scheduler` | Lên lịch và chạy task (LocalExecutor chạy task ngay trên scheduler) |
| `airflow-dag-processor` | Parse DAG từ thư mục DAG |
| `airflow-triggerer` | Xử lý deferrable operators |
| `airflow-init` | Khởi tạo DB, tạo user admin, dựng sẵn venv business logic |

Business logic được chạy qua `ExternalPythonOperator` / `@task.external_python`, trỏ tới
interpreter của venv riêng thay vì Python của Airflow.

```
┌───────────────────────────────┐
│ Airflow (image apache/airflow)│  ← Python của Airflow, KHÔNG đụng vào
│  scheduler / apiserver / ...  │
│                               │
│   @task.external_python  ─────┼──► /opt/airflow/pyenv/venv/bin/python3
└───────────────────────────────┘        ↑ venv riêng cho business logic
                                        (pip install thoải mái, persist qua volume)
```

## Yêu cầu

- Docker + Docker Compose
- (Khuyến nghị deploy production trên Linux server)

## Cấu trúc thư mục

```
.
├── docker-compose.yaml      # Định nghĩa toàn bộ service
├── .env                     # Cấu hình (KHÔNG commit lên git)
├── dags/                    # Thư mục DAG mặc định (bundle "dags-folder")
│   └── demo.py              # Pipeline mẫu (CLI + Airflow)
```

## Cấu hình (`.env`)

Các biến quan trọng trong `.env`:

| Biến | Ý nghĩa |
|---|---|
| `AIRFLOW_VERSION` | Version Airflow — nguồn duy nhất, dùng cho cả image lẫn venv |
| `EXTERNAL_PYTHON` | Interpreter của venv business logic mà DAG sẽ dùng |
| `DAGS_FOLDER_2` / `DAGS_FOLDER_3` | Thư mục DAG phụ trên host → DAG bundle riêng (tùy chọn) |
| `GIT1_*` / `GIT2_*` | Cấu hình git DAG bundle qua SSH (tùy chọn, mặc định tắt) |
| `AIRFLOW_UID` | UID chạy container (chạy `echo $(id -u)` để lấy) |
| `FERNET_KEY` | Key mã hoá connection/password — **phải đổi** trước khi dùng |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | Thông tin Postgres |
| `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` | Tài khoản admin web UI |

> ⚠️ Trước khi deploy, hãy thay tất cả các giá trị `REPLACE_WITH_...` và sinh `FERNET_KEY` mới:
> ```shell
> python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
> ```

## Deployment

Khởi tạo lần đầu (migrate DB, tạo user admin, dựng venv business logic):

```shell
docker compose up airflow-init
```

Chạy toàn bộ stack:

```shell
docker compose up -d
```

Mở Web UI tại http://127.0.0.1:8080 và đăng nhập bằng tài khoản admin trong `.env`.

## Viết DAG

Mỗi pipeline là một file Python chạy được theo 2 chế độ. Xem `dags/demo.py` làm mẫu:

```python
from __future__ import annotations

import os
from datetime import datetime

EXTERNAL_PYTHON = os.getenv("EXTERNAL_PYTHON", "/opt/airflow/pyenv/venv/bin/python3")


# --- Business logic: hàm self-contained (import nằm BÊN TRONG hàm) ---
def extract_customers():
    import pandas as pd
    print("Extract customers")


def clean_customers():
    print("Clean customers")


def main():
    extract_customers()
    clean_customers()


# --- Airflow mode: TaskFlow API + @task.external_python ---
try:
    from airflow.sdk import dag, task

    @dag(
        dag_id="customer_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["customer"],
    )
    def customer_pipeline():
        extract = task.external_python(python=EXTERNAL_PYTHON)(extract_customers)
        clean = task.external_python(python=EXTERNAL_PYTHON)(clean_customers)
        extract() >> clean()

    customer_pipeline()
except ModuleNotFoundError:
    pass


if __name__ == "__main__":
    main()
```

Chạy nhanh bằng CLI để test logic, không cần Airflow:

```shell
python dags/demo.py
```

**Lưu ý quan trọng khi dùng `@task.external_python`**: thân hàm phải *self-contained* —
mọi `import` đặt bên trong hàm, không tham chiếu biến/hàm ở scope ngoài. Lý do: decorator lấy
*source code* của đúng hàm đó (`inspect.getsource`) rồi chạy bằng interpreter của venv, nên các
tên ngoài hàm sẽ không tồn tại trong môi trường thực thi.

## Quản lý Python environment

Khi deploy, một venv mặc định đã được tạo sẵn tại `/opt/airflow/pyenv/venv`, đã cài
`apache-airflow` cùng version với image. Venv này nằm trên Docker volume nên **persist qua restart**.

### Cài thêm package

```shell
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv/bin/pip install <package>
```

Package có hiệu lực ngay ở lần chạy task kế tiếp — không cần restart hay rebuild.

### Tạo venv riêng cho pipeline đặc biệt

```shell
# Đặt trong /opt/airflow/pyenv/ để nằm trên volume persist
docker compose exec airflow-scheduler python -m venv /opt/airflow/pyenv/venv-special

# Cài apache-airflow (cùng version với image, để ExternalPythonOperator có context)
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv-special/bin/pip install --upgrade pip
docker compose exec airflow-scheduler /opt/airflow/pyenv/venv-special/bin/pip install "apache-airflow==3.2.2"
```

Sau đó trỏ DAG tới interpreter của venv mới:

```python
EXTERNAL_PYTHON = "/opt/airflow/pyenv/venv-special/bin/python3"
```

## Thư mục DAG & nhiều nguồn (DAG bundles)

Thư mục DAG chính mặc định là `./dags` trong repo (bundle `dags-folder`), không cần cấu hình gì.

Khi DAG nằm rải rác ở các thư mục khác trên máy, mỗi thư mục được khai báo là một **DAG bundle**
riêng — một tính năng của Airflow 3. Mỗi bundle có tên riêng và hiển thị tách biệt trong Web UI
(cột *Bundle*), tiện theo dõi DAG đến từ nguồn nào.

Chỉ cần khai báo path các thư mục phụ trong `.env`:

```shell
DAGS_FOLDER_2=/Users/me/marketing/dags  # bundle "dags-folder-2"
DAGS_FOLDER_3=/Users/me/finance/dags    # bundle "dags-folder-3"
```

Cơ chế: mỗi thư mục phụ được mount vào một path **ngoài** `/opt/airflow/dags`
(`/opt/airflow/extra-dags/dags-folder-N`) để bundle chính không quét trùng, rồi đăng ký thành bundle qua
biến `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST` trong `docker-compose.yaml`. Slot nào không
khai báo sẽ tự mount một thư mục rỗng (vô hại).

Cần thêm nhiều hơn: thêm một mount mới trong `docker-compose.yaml` và thêm một entry tương ứng
vào `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST`.

Sau khi đổi, tạo lại container:

```shell
docker compose up -d
```

Kiểm tra DAG thuộc bundle nào:

```shell
docker compose exec airflow-scheduler airflow dags list
```

> ⚠️ `dag_id` phải là **duy nhất trên toàn bộ các bundle**. Hai DAG trùng `dag_id` (kể cả ở thư
> mục khác nhau) sẽ gây import error.

> Lưu ý (macOS): nếu thư mục nằm ngoài các path Docker Desktop share sẵn (vd ngoài `/Users`),
> cần thêm nó vào **Docker Desktop → Settings → Resources → File Sharing**.

### Bundle kéo trực tiếp từ git (SSH)

Ngoài thư mục local, một bundle có thể kéo DAG **trực tiếp từ git remote** (`GitDagBundle`) —
không cần mount, Airflow tự clone và pull theo nhánh sau mỗi `refresh_interval`. Đây là cách
production-grade nhất: dev push lên git, Airflow tự cập nhật.

Có sẵn **2 slot git độc lập** (`git1`, `git2`), **mặc định tắt** (không gây lỗi). Mỗi slot bật
riêng bằng cách **bỏ comment các dòng `GITn_*`** trong `.env` và điền giá trị thật:

```shell
# Slot 1 -> bundle "git1"
GIT1_REPO_URL=git@github.com:your-org/repo-a.git  # SSH URL của repo
GIT1_REF=main                                     # nhánh/tag theo dõi (mặc định main)
GIT1_SUBDIR=dags                                  # thư mục chứa DAG trong repo (mặc định dags)
GIT1_SSH_KEY=/Users/me/.ssh/id_ed25519_a          # đường dẫn private key trên host

# Slot 2 -> bundle "git2" (tương tự, dùng GIT2_*)
```

Rồi tạo lại container:

```shell
docker compose up -d
```

Cơ chế: bundle `gitN` chỉ được thêm vào danh sách khi `GITn_REPO_URL` có giá trị (dùng cú pháp
`${VAR:+...}` của Compose). Để trống → không có bundle đó, không lỗi. Khi bật, private key được
mount read-only vào `/opt/airflow/git-ssh/gitN_key` và dùng qua connection `gitN`. Hai slot dùng
key riêng nên có thể trỏ tới repo khác nhau với deploy key khác nhau.

Cần nhiều hơn 2 git repo: thêm slot `git3` theo đúng pattern (một entry `${GIT3_REPO_URL:+...}`
trong list, một connection `AIRFLOW_CONN_GIT3`, một mount key) trong `docker-compose.yaml`.

Kiểm tra clone thành công:

```shell
docker compose logs airflow-dag-processor | grep -iE "git1|git2"
docker compose exec airflow-scheduler airflow dags list   # DAG sẽ hiện với bundle = git1 / git2
```

> Private key cần quyền `600` và không được commit lên git.

## Một số lệnh vận hành

```shell
# Xem danh sách DAG
docker compose exec airflow-scheduler airflow dags list

# Kiểm tra lỗi import DAG
docker compose exec airflow-scheduler airflow dags list-import-errors

# Trigger thủ công một DAG
docker compose exec airflow-scheduler airflow dags trigger <dag_id>

# Xem log realtime
docker compose logs -f airflow-scheduler

# Dừng stack
docker compose down

# Dừng và xoá cả volume (mất venv + metadata DB)
docker compose down -v
```

## Executor

Sử dụng **LocalExecutor** — task chạy trực tiếp trên scheduler. Phù hợp cho mô hình một server;
scheduler được cấp nhiều tài nguyên hơn các service khác (xem `docker-compose.yaml`).