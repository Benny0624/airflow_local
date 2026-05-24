# dbt_audit_helper DAG

用 [dbt-labs/audit_helper](https://github.com/dbt-labs/dbt-audit-helper) 的 `compare_all_columns` macro 比較兩張資料表的差異，分別測試 `summarize=true`（差異統計）與 `summarize=false`（原始差異資料）兩種模式。

## 目錄結構

```
dbt_audit_helper/
├── dbt_audit_helper_dag.py   # Airflow DAG
├── dbt_project.yml
├── packages.yml
├── profiles.yml
├── configs/
│   └── comparisons.py        # 所有比較設定的唯一入口
├── seeds/                    # 測試用假資料
├── models/
│   ├── sources/              # 自訂來源 SQL（dbt 不解析）
│   │   ├── source_data/
│   │   │   ├── a.sql         # 來源表 A 的 query
│   │   │   └── b.sql         # 來源表 B 的 query
│   │   └── orders/
│   │       ├── a.sql
│   │       └── b.sql
│   └── audits/               # dbt models（generic，所有比較共用）
│       ├── audit_summary.sql # summarize=true
│       └── audit_detail.sql  # summarize=false
└── templates/
    ├── compare_summary.sql   # 查詢 audit 結果的 SQL
    └── compare_detail.sql
```

## YAML 設定說明

### `dbt_project.yml` — 專案設定

```yaml
name: audit_helper_test        # 需與 profiles.yml 的 profile 名稱一致
profile: audit_helper_test

model-paths: ["models/audits"] # 只解析 audits/，sources/ 為純 SQL 文件
seed-paths: ["seeds"]
macro-paths: ["macros"]

seeds:
  audit_helper_test:
    +column_types:             # 強制指定 CSV 欄位型別
      id: integer
      amount: numeric(10,2)

models:
  audit_helper_test:
    +materialized: table       # audit 結果寫成實體表，方便查詢
```

### `packages.yml` — dbt 套件

```yaml
packages:
  - package: dbt-labs/audit_helper
    version: [">=0.9.0"]
```

`dbt deps` 執行後下載到 `dbt_packages/`。

### `profiles.yml` — 資料庫連線

```yaml
audit_helper_test:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST', 'postgres') }}"
      user: "{{ env_var('POSTGRES_USER', 'airflow') }}"
      password: "{{ env_var('POSTGRES_PASSWORD', 'airflow') }}"
      port: 5432
      dbname: "{{ env_var('POSTGRES_DB', 'airflow') }}"
      schema: public          # audit 結果表寫到這個 schema
      threads: 1
```

**注意：**
- `schema` 只控制 audit 結果的輸出位置，被比較的來源表由 `configs/comparisons.py` 指定
- 要連不同資料庫，在 `outputs` 下新增 target，執行時加 `--target <name>` 切換
- 換資料庫類型需在 `pyproject.toml` 裝對應 adapter（`dbt-bigquery`、`dbt-snowflake` 等）

**多 target 範例：**

```yaml
outputs:
  dev:
    type: postgres
    host: postgres
    dbname: airflow
    schema: public

  prod:
    type: postgres
    host: prod-db.internal
    dbname: analytics
    schema: audit_results
```

切換：`dbt run --target prod --profiles-dir .`

## DAG 流程

```
dbt_deps → dbt_seed ┬→ run_summary_{name} → log_summary_{name}
                    └→ run_detail_{name}  → log_detail_{name}
                    （每個 COMPARISONS 項目各跑一組，並行）
```

| Task | 說明 |
|------|------|
| `dbt_deps` | 安裝 audit_helper 套件 |
| `dbt_seed` | 載入 CSV 測試資料 |
| `run_summary_{name}` | 執行 `audit_summary` model（summarize=true） |
| `run_detail_{name}` | 執行 `audit_detail` model（summarize=false） |
| `log_summary_{name}` | 查詢並印出 summary 結果 |
| `log_detail_{name}` | 查詢並印出 detail 結果 |

## 新增一組比較

### Step 1：在 `models/sources/` 新增資料夾與 SQL

```
models/sources/{name}/
├── a.sql   ← 來源表 A 的 query
└── b.sql   ← 來源表 B 的 query
```

範例（直接查整張表）：

```sql
-- models/sources/my_table/a.sql
SELECT *
FROM staging.my_table

-- models/sources/my_table/b.sql
SELECT *
FROM production.my_table
```

範例（加 filter）：

```sql
-- models/sources/orders/a.sql
SELECT *
FROM staging.orders
WHERE created_at >= '2024-01-01'
```

> `sources/` 不是 dbt model，純 SQL 文件供人閱讀與維護，實際執行由 `configs/comparisons.py` 的 schema/table 設定驅動。

### Step 2：在 `configs/comparisons.py` 新增設定

```python
COMPARISONS = [
    ...
    ComparisonConfig(
        name="my_table",           # 對應 sources/ 資料夾名稱，也是結果表後綴
        schema_a="staging",
        table_name_a="my_table",
        schema_b="production",
        table_name_b="my_table",
        primary_key="id",
    ),
]
```

DAG 與 audit models 不需要改動。

## 輸出表說明

結果表寫入 `profiles.yml` 的 `schema`，命名規則為 `audit_summary_{name}` 和 `audit_detail_{name}`。

### 欄位定義

| 欄位 | 說明 |
|------|------|
| `column_name` | 被比較的欄位名稱 |
| `perfect_match` | 兩邊值完全一樣 |
| `null_in_a` | A 表該欄位為 NULL |
| `null_in_b` | B 表該欄位為 NULL |
| `missing_from_a` | 該筆 primary key 只在 B 表，A 表沒有 |
| `missing_from_b` | 該筆 primary key 只在 A 表，B 表沒有 |
| `conflicting_values` | 兩邊都有這筆，但值不同 |

### `summarize=true`（`audit_summary_{name}`）

每個欄位一列，數字為筆數統計：

```
 column_name | perfect_match | null_in_a | null_in_b | missing_from_a | missing_from_b | conflicting_values
-------------+---------------+-----------+-----------+----------------+----------------+--------------------
 AMOUNT      |             3 |         0 |         0 |              1 |              1 |                  1
 STATUS      |             3 |         0 |         0 |              1 |              1 |                  1
 ID          |             4 |         0 |         0 |              1 |              1 |                  0
 NAME        |             4 |         0 |         0 |              1 |              1 |                  0
```

### `summarize=false`（`audit_detail_{name}`）

以 `primary_key + column_name` 為單位，每個欄位一列，boolean 標記差異類型：

```
 primary_key | column_name | perfect_match | null_in_a | null_in_b | missing_from_a | missing_from_b | conflicting_values
-------------+-------------+---------------+-----------+-----------+----------------+----------------+--------------------
           2 | AMOUNT      | f             | f         | f         | f              | f              | t
           3 | STATUS      | f             | f         | f         | f              | f              | t
           5 | ID          | f             | f         | f         | f              | t              | f
           6 | ID          | f             | f         | f         | t              | f              | f
```
