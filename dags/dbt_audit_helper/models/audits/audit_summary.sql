{{ config(alias='audit_summary_' ~ var('name')) }}

{% set a_rel = api.Relation.create(
    database=target.database,
    schema=var('schema_a'),
    identifier=var('table_name_a')
) %}
{% set b_rel = api.Relation.create(
    database=target.database,
    schema=var('schema_b'),
    identifier=var('table_name_b')
) %}

{{ audit_helper.compare_all_columns(
    a_relation=a_rel,
    b_relation=b_rel,
    primary_key=var('primary_key', 'id'),
    summarize=true
) }}
