{{ config(
    materialized='table',
    schema='br_cgu_terceirizados',
    alias='silver',
    pre_hook="ATTACH 'terceirizados-bronze.duckdb' AS bronze_db"
) }}

SELECT
    CAST(id_terc AS VARCHAR) AS id,
    CAST(sg_orgao_sup_tabela_ug AS VARCHAR) AS sigla_orgao_superior,
    CAST(cd_ug_gestora AS VARCHAR) AS id_unidade_gestora,
    CAST(nm_ug_tabela_ug AS VARCHAR) AS nome_unidade_gestora,
    CAST(sg_ug_gestora AS VARCHAR) AS sigla_unidade_gestora,
    CAST(nr_contrato AS VARCHAR) AS id_contrato,
    CAST(nm_razao_social AS VARCHAR) AS razao_social,
    CAST(nr_cpf AS VARCHAR) AS cpf,
    CAST(nm_terceirizado AS VARCHAR) AS nome,
    CAST(nm_categoria_profissional AS VARCHAR) AS id_categoria_profissional,
    CAST(nm_escolaridade AS VARCHAR) AS escolaridade,
    CAST(NULLIF(nr_jornada, 'NI  ') AS INT) AS jornada,
    CAST(nm_unidade_prestacao AS VARCHAR) AS descricao_unidade_prestacao,
    ROUND(CAST(REPLACE(vl_mensal_salario, ',', '.') AS FLOAT),3) AS salario,
    ROUND(CAST(REPLACE(vl_mensal_custo, ',', '.') AS FLOAT),3) AS custo,
    CAST(Num_Mes_Carga AS INT) AS mes,
    CAST(Ano_Carga AS INT) AS ano,
    CAST(sg_orgao AS VARCHAR) AS sigla_orgao,
    CAST(nm_orgao AS VARCHAR) AS nome_orgao,
    CAST(cd_orgao_siafi AS VARCHAR) AS id_siafi,
    CAST(cd_orgao_siape AS VARCHAR) AS id_siape,

FROM bronze_db.br_cgu_terceirizados.bronze