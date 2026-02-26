{{ config(
    materialized='incremental',
    schema='br_cgu_terceirizados',
    alias='gold_policia_federal',
    pre_hook="ATTACH 'duckdb/terceirizados-silver.duckdb' AS silver_db"
) }}

SELECT 
    *
FROM silver_db.silver
WHERE nome_unidade_gestora = 'DEPARTAMENTO DE POLICIA RODOVIARIA FEDERAL'