{{ config(
    materialized='table',
    schema='br_cgu_terceirizados',
    alias='gold',
    pre_hook="ATTACH 'duckdb/terceirizados-silver.duckdb' AS silver_db"
) }}

SELECT 
    id, 
    sigla_orgao_superior,
    cnpj_empresa,
    cpf
FROM silver_db.br_cgu_terceirizados.silver