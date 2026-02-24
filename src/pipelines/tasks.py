import pandas as pd
import requests
import os
from src.pipelines.constants import constants
from src.utils.setup import max_date_duckdb, log
from prefect import task
from typing import List


@task
def check_for_update(format: List[str]) -> bool:
    #date = max_date_duckdb()
    for formato in format:

        #filename = f'{date}{formato}'
        url = f'{constants.URL}202501.xlsx'
        log(f'Checking for update at {url}...')
        if requests.get(
            url,
            cookies=constants.COOKIES,
            headers=constants.HEADERS,
            timeout=10
        ).status_code == 200:
            log(f'URL  exists: {url}!!! Starting pipeline...')
            return True

@task
def download_data(format :  List[str] = ['.csv', '.xlsx']):
    #date = max_date_duckdb()
    for formato in format:
        #filename = f'{date}{formato}'
        url = f'{constants.URL}202501{formato}'
        
        try:
            r = requests.get(
                url,
                cookies=constants.COOKIES,
                headers=constants.HEADERS,
                timeout=10
            )

            if r.status_code == 200:
                log(f'Arquivo encontrado: {url}')
                break

            elif r.status_code == 404:
                log(f'Não encontrado (404): {url}')
                continue

            else:
                log(f'Status inesperado {r.status_code}: {url}')

        except requests.exceptions.RequestException as e:
            log(f"Erro de conexão em {url}: {e}")
            return

    os.makedirs('input', exist_ok=True)
    log(f'Baixando arquivo para input/202501{formato}...')
    with open(f'input/202501{formato}', 'wb') as f:
        f.write(r.content)


@task
def ingest_and_partition(input : str, output : str) -> None:
    for file in os.listdir(input):
        log(f"Processing file: {file}...")

        filepath = os.path.join(input, file)

        if file.endswith('.csv'):
            cols = pd.read_csv(
                filepath,
                sep=';',
                encoding='latin1',
                nrows=0
            ).columns.tolist()

        elif file.endswith('.xlsx'):
            cols = pd.read_excel(
                filepath,
                dtype=str,
                nrows=0
            ).columns.tolist()


        if cols == constants.COLUMNS:
            log("Columns match, proceeding to read full dataset.")

            if file.endswith('.csv'):
                df = pd.read_csv(filepath, sep=';', encoding='latin1', dtype=str)

            else:
                df = pd.read_excel(filepath, dtype=str)

        else:
            log(f"Columns do not match for {file}, forcing schema...")

            if file.endswith('.csv'):
                df = pd.read_csv(
                    filepath,
                    sep=';',
                    encoding='utf-8',
                    dtype=str,
                    names=constants.COLUMNS,
                    header=None
                )

            else:
                df = pd.read_excel(
                    filepath,
                    dtype=str,
                    names=constants.COLUMNS,
                    header=None
                )

        log(f"Loaded {file} with shape {df.shape}")
        
        for ano in df['Ano_Carga'].unique():
            for mes in df['Num_Mes_Carga'].unique():
                particionamento = f"ano={ano}/mes={mes}"
                log(f"Creating partition: {particionamento}...")
                os.makedirs(output, exist_ok=True)
                
                df.to_parquet(f"{output}/terceirizados_{ano}{mes.zfill(2)}.parquet", index=False,)