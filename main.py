import pandas as pd
import os

COLUMNS = [
        'id_terc',
        'sg_orgao_sup_tabela_ug',
        'cd_ug_gestora',
        'nm_ug_tabela_ug',
        'sg_ug_gestora',
        'nr_contrato',
        'nr_cnpj',
        'nm_razao_social',
        'nr_cpf',
        'nm_terceirizado',
        'nm_categoria_profissional',
        'nm_escolaridade',
        'nr_jornada',
        'nm_unidade_prestacao',
        'vl_mensal_salario',
        'vl_mensal_custo',
        'Num_Mes_Carga',
        'Mes_Carga',
        'Ano_Carga',
        'sg_orgao',
        'nm_orgao',
        'cd_orgao_siafi',
        'cd_orgao_siape'
    ]

for x in os.listdir('input'):
    print(f"Processing file: {x}...")

    filepath = os.path.join('input', x)

    # --- Lê apenas as colunas ---
    if x.endswith('.csv'):
        cols = pd.read_csv(
            filepath,
            sep=';',
            encoding='latin1',
            nrows=0
        ).columns.tolist()

    elif x.endswith('.xlsx'):
        cols = pd.read_excel(
            filepath,
            dtype=str,
            nrows=0
        ).columns.tolist()

    # --- Compara ---
    if cols == COLUMNS:
        print("Columns match, proceeding to read full dataset.")

        if x.endswith('.csv'):
            df = pd.read_csv(filepath, sep=';', encoding='latin1', dtype=str)

        else:
            df = pd.read_excel(filepath, dtype=str)

    else:
        print(f"Columns do not match for {x}, forcing schema...")

        if x.endswith('.csv'):
            df = pd.read_csv(
                filepath,
                sep=';',
                encoding='latin1',
                dtype=str,
                names=COLUMNS,
                header=None
            )

        else:
            df = pd.read_excel(
                filepath,
                dtype=str,
                names=COLUMNS,
                header=None
            )

    print(f"Loaded {x} with shape {df.shape}")
    
    for ano in df['Ano_Carga'].unique():
        for mes in df['Num_Mes_Carga'].unique():
            breakpoint
            particionamento = f"ano={ano}/mes={mes}"
            print(f"Creating partition: {particionamento}...")
            if os.makedirs(f"output/{particionamento}", exist_ok=True):
                print("Diretório já existe, continuando...")
                pass

            df_particionado = df[(df['Ano_Carga'] == ano) & (df['Num_Mes_Carga'] == mes)]
            
            df_particionado.to_parquet(f"output/{particionamento}/data.parquet", index=False,)    