# config_driven_dag

## BranchPythonOperator 原理

Airflow 的 schedule 是 **DAG 層級**，不是 task 層級。
要讓不同 task 有各自的執行頻率，做法是：

1. DAG 以最小時間單位（`@hourly`）觸發
2. 每次 DAG Run 啟動時，Airflow 為所有 task 建立 TaskInstance，初始狀態為 `none`
3. `branch` task 執行，呼叫 `python_callable`，根據當前 execution time 比對各 task 的 cron
4. BranchPythonOperator 將**沒被選中**的 task 直接寫入 `skipped` 狀態進 DB
5. Scheduler 只執行剩下狀態為 `none` 的 task

```
DAG Run (@hourly)
    │
   branch  ← 比對 config 裡各 task 的 cron
    ├── run__task_a  (符合 → 執行 / 不符合 → skipped)
    ├── run__task_b  (符合 → 執行 / 不符合 → skipped)
    ├── run__task_c  (符合 → 執行 / 不符合 → skipped)
    └── no_op        (所有 task 都不符合時走這條)
         │
        end
```

**Scheduler 不會持續輪詢 task**，每小時只觸發一次 DAG Run，routing 判斷在 branch task 執行當下一次決定。

### 適用場景

混用 hourly + daily task 時特別合適：
- hourly task：每次 DAG Run 都被放行
- daily task：只有對到時間點的那次 DAG Run 才執行，其餘 23 次走 `skipped`

---

## 須新增的套件

`apache/airflow:2.11.0` image 不含 `croniter`，需要額外安裝。

編輯專案根目錄的 `.env`：

```
_PIP_ADDITIONAL_REQUIREMENTS=croniter
```

重啟 container：

```bash
docker compose down
docker compose up -d
```

---

## 新增 Task

在 `config/` 下新增一個 `.properties` 檔即可，不需要改 DAG 程式碼。

**格式：**

```properties
task_id=my_new_task
schedule=0 12 * * *
description=每天中午跑
param=whatever_you_need
```

| 欄位 | 說明 |
|------|------|
| `task_id` | task 的唯一識別，會對應到 Airflow UI 上的 task 名稱（`run__<task_id>`） |
| `schedule` | 標準 cron 格式，決定這個 task 在哪些 DAG Run 裡執行 |
| `description` | 說明用，會印在 log 裡 |
| `param` | 傳給 worker 的自訂參數，可依需求擴充欄位 |

存檔後 Airflow 會在下次 DAG Run 自動偵測到新 task（熱更新，無需重啟）。

---

## 目錄結構

```
dags/config_driven_dag/
├── config_driven_dag.py   # DAG 主程式
├── config/
│   ├── task_a.properties  # 工作日 09:00
│   ├── task_b.properties  # 每 4 小時
│   └── task_c.properties  # 每天 18:00
└── README.md
```
