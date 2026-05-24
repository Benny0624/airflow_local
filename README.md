# Airflow 2.11 本地測試環境

## 快速啟動

```bash
# 第一次啟動（初始化 DB + 建立 admin 帳號）
docker compose up airflow-init

# 啟動 webserver + scheduler
docker compose up -d

# 停止
docker compose down

# 完整清除（含 DB volume）
docker compose down -v
```

啟動後開啟 http://localhost:8080
帳號 / 密碼：`admin` / `admin`

## 加裝 Python 套件

套件透過 `pyproject.toml` + `uv` 在 build 階段安裝，不在 container 啟動時安裝。

**Step 1：編輯 `pyproject.toml`，在 `dependencies` 加入套件**

```toml
[project]
dependencies = [
    "croniter==2.0.5",
    "pyyaml",
    "apache-airflow-providers-google",
]
```

**Step 2：重新 build image 並啟動**

```bash
docker compose build
docker compose up -d
```

## 目錄結構

```
airflow_local/
├── dags/                    # DAG 檔案放這裡，熱更新
│   └── example_operators.py # BashOperator / PythonOperator / BranchPythonOperator 範例
├── logs/                    # Task log 輸出
├── plugins/                 # 自定義 plugin
├── config/                  # airflow.cfg override
├── docker-compose.yml
├── Dockerfile               # 繼承官方 image，用 uv 安裝套件
├── pyproject.toml           # Python 套件清單
└── .env                     # UID / 帳密
```

## 常用指令

```bash
# 進入 CLI 容器
docker compose run --rm airflow-cli bash

# 手動觸發 DAG
docker compose run --rm airflow-cli airflow dags trigger example_operators

# 查看 task log
docker compose run --rm airflow-cli airflow tasks logs example_operators bash_task <execution_date>
```
