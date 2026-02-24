{{ config(materialized='table',
        schema='br_cgu_terceirizados',
        alias='bronze') }}

SELECT *
FROM read_parquet(
    'gs://br-cgu-terceirizados/terceirizados/*.parquet'
)
