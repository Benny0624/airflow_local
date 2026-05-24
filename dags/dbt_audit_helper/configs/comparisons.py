from dataclasses import dataclass
import os

DBT_DIR = "/opt/airflow/dags/dbt_audit_helper"
TEMPLATES_DIR = os.path.join(DBT_DIR, "templates")
RESULT_SCHEMA = "public"


@dataclass
class ComparisonConfig:
    name: str
    schema_a: str
    table_name_a: str
    schema_b: str
    table_name_b: str
    primary_key: str


COMPARISONS = [
    ComparisonConfig(
        name="source_data",
        schema_a="public",
        table_name_a="source_data",
        schema_b="public",
        table_name_b="target_data",
        primary_key="id",
    ),
    ComparisonConfig(
        name="orders",
        schema_a="public",
        table_name_a="orders_source",
        schema_b="public",
        table_name_b="orders_target",
        primary_key="order_id",
    ),
]
