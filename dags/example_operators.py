"""
Operator 測試 DAG — 可在這裡隨意加減想測的 Operator
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

def _print_hello(**context):
    print(f"Hello from PythonOperator! execution_date={context['ds']}")
    return "done"

def _branch(**context):
    # 示範 BranchPythonOperator：依星期幾決定走哪條路
    weekday = datetime.now().weekday()
    return "weekday_task" if weekday < 5 else "weekend_task"

with DAG(
    dag_id="example_operators",
    start_date=datetime(2025, 1, 1),
    schedule=None,       # 手動觸發
    catchup=False,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo 'BashOperator OK' && date",
    )

    python_task = PythonOperator(
        task_id="python_task",
        python_callable=_print_hello,
    )

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=_branch,
    )

    weekday_task = EmptyOperator(task_id="weekday_task")
    weekend_task = EmptyOperator(task_id="weekend_task")

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> [bash_task, python_task] >> branch >> [weekday_task, weekend_task] >> end
