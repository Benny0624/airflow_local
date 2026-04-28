"""
Config-driven DAG：從 config/*.properties 動態產生 task，
各 task 有自己的 cron schedule，由 BranchPythonOperator 決定哪些該跑。

DAG 本身跑 @hourly，branch 負責依各 task 的 schedule 過濾。
需要：croniter（見 README.md）
"""
import glob
import os
from datetime import datetime

from croniter import croniter
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "config")


def _load_properties(path: str) -> dict:
    props = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key, _, value = line.partition("=")
                props[key.strip()] = value.strip()
    return props


def _load_configs() -> list[dict]:
    return [
        _load_properties(p)
        for p in sorted(glob.glob(os.path.join(CONFIG_DIR, "*.properties")))
    ]


CONFIGS = _load_configs()


def _branch(**context):
    """比對各 config 的 cron 與當前 execution time，回傳該跑的 task_id list。"""
    execution_time = context["data_interval_start"]
    active_tasks = [
        f"run__{cfg['task_id']}"
        for cfg in CONFIGS
        if croniter.match(cfg["schedule"], execution_time)
    ]
    return active_tasks if active_tasks else ["no_op"]


def _make_worker(cfg: dict):
    """用 closure 綁定各自的 config，避免所有 task 共用同一個參考。"""
    def _worker(**context):
        print(f"[{cfg['task_id']}] {cfg['description']} | param={cfg['param']}")
        print(f"[{cfg['task_id']}] execution_time={context['data_interval_start']}")
        # ← 把你真正要跑的邏輯放這裡
    return _worker


with DAG(
    dag_id="config_driven_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["config-driven"],
) as dag:

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=_branch,
    )

    no_op = EmptyOperator(task_id="no_op")

    task_ops = [
        PythonOperator(
            task_id=f"run__{cfg['task_id']}",
            python_callable=_make_worker(cfg),
        )
        for cfg in CONFIGS
    ]

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    branch >> [no_op, *task_ops] >> end
