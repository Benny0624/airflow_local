{{ config(materialized='ephemeral') }}

SELECT *
FROM public.orders_target
