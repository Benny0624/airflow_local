SELECT *
FROM {{ schema }}.audit_summary_{{ table_name }}
ORDER BY 1
