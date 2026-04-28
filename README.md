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

`apache/airflow:2.11.0` image 只預裝 Airflow 核心套件。
DAG 裡用到 image 沒有的套件（例如 `croniter`、`pyyaml`）就需要額外安裝。

### 方法：透過 `_PIP_ADDITIONAL_REQUIREMENTS`

Airflow 官方 image 支援這個環境變數，container 啟動時會自動 `pip install` 指定的套件。

**Step 1：編輯 `.env`，在 `_PIP_ADDITIONAL_REQUIREMENTS` 填入套件名（空白分隔）**

```
_PIP_ADDITIONAL_REQUIREMENTS=croniter pyyaml
```

多個套件可以同時填，也可以指定版本：

```
_PIP_ADDITIONAL_REQUIREMENTS=croniter==2.0.5 pyyaml apache-airflow-providers-google
```

**Step 2：重啟 container 讓設定生效**

```bash
docker compose down
docker compose up -d
```

> 注意：每次 container 啟動都會重新 pip install，冷啟動會慢幾秒。
> 若套件量多、啟動速度是問題，考慮自己 build image（繼承官方 image 再 `pip install`）。

## 目錄結構

```
airflow_local/
├── dags/                    # DAG 檔案放這裡，熱更新
│   └── example_operators.py # BashOperator / PythonOperator / BranchPythonOperator 範例
├── logs/                    # Task log 輸出
├── plugins/                 # 自定義 plugin
├── config/                  # airflow.cfg override
├── docker-compose.yml
└── .env                     # UID / 帳密 / 額外套件
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
