{{ config(
    materialized='incremental',
    schema='br_cgu_terceirizados',
    alias='gold_policia_federal',
    pre_hook="ATTACH 'duckdb/terceirizados-silver.duckdb' AS silver_db"
) }}

SELECT 
    id, 
    sigla_orgao_superior,
    cnpj,
    cpf
FROM silver_db.silver