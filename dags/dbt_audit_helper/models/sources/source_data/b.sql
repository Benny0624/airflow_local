{{ config(materialized='ephemeral') }}

SELECT *
FROM public.target_data
