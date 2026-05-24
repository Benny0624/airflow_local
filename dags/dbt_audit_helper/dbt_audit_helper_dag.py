from __future__ import annotations

import os
import psycopg2
from datetime import datetime

import sys
sys.path.insert(0, "/opt/airflow/dags/dbt_audit_helper")

from configs.comparisons import COMPARISONS, ComparisonConfig, RESULT_SCHEMA, DBT_DIR, TEMPLATES_DIR  # noqa: E402

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _dbt_cmd(cmd: str, comp: ComparisonConfig | None = None) -> str:
    base = f"cd {DBT_DIR} && dbt {cmd} --profiles-dir ."
    if comp is None:
        return base
    vars_json = (
        f'{{"name": "{comp.name}"'
        f', "schema_a": "{comp.schema_a}"'
        f', "table_name_a": "{comp.table_name_a}"'
        f', "schema_b": "{comp.schema_b}"'
        f', "table_name_b": "{comp.table_name_b}"'
        f', "primary_key": "{comp.primary_key}"}}'
    )
    return f"{base} --vars '{vars_json}'"


def _log_results(
    template_file: str,
    label: str,
    table_name: str,
    **context,
) -> None:
    with open(os.path.join(TEMPLATES_DIR, template_file)) as f:
        sql = (
            f.read()
            .replace("{{ schema }}", RESULT_SCHEMA)
            .replace("{{ table_name }}", table_name)
        )

    conn = psycopg2.connect(
        host="postgres", port=5432, dbname="airflow",
        user="airflow", password="airflow",
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

        sep = "=" * 70
        print(f"\n{sep}")
        print(f"  {label}  [{table_name}]")
        print(sep)
        print(" | ".join(cols))
        print("-" * 70)
        for row in rows:
            print(" | ".join(str(v) for v in row))
        print(f"{sep}\n")
    finally:
        conn.close()


with DAG(
    dag_id="dbt_audit_helper",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "audit"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=_dbt_cmd("deps"),
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=_dbt_cmd("seed"),
    )

    for comp in COMPARISONS:
        run_summary = BashOperator(
            task_id=f"run_summary_{comp.name}",
            bash_command=_dbt_cmd("run --select audit_summary", comp),
        )

        run_detail = BashOperator(
            task_id=f"run_detail_{comp.name}",
            bash_command=_dbt_cmd("run --select audit_detail", comp),
        )

        log_summary = PythonOperator(
            task_id=f"log_summary_{comp.name}",
            python_callable=_log_results,
            op_kwargs={
                "template_file": "compare_summary.sql",
                "label":         "summarize=true — 每欄差異統計",
                "table_name":    comp.name,
            },
        )

        log_detail = PythonOperator(
            task_id=f"log_detail_{comp.name}",
            python_callable=_log_results,
            op_kwargs={
                "template_file": "compare_detail.sql",
                "label":         "summarize=false — 有差異的原始資料",
                "table_name":    comp.name,
            },
        )

        dbt_seed >> [run_summary, run_detail]
        run_summary >> log_summary
        run_detail >> log_detail

    dbt_deps >> dbt_seed
