{{ config(materialized='ephemeral') }}

SELECT *
FROM public.source_data
