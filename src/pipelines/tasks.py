import pandas as pd
import requests
import os
from src.pipelines.constants import constants
from src.utils.setup import max_date_duckdb, log, build_filenames
from prefect import task
from typing import List
from bs4 import BeautifulSoup as bs


@task
def check_for_update_and_download(format: List[str], date : str) -> bool:
    """
    Check if updated data files are available at the remote URL.
    
    Queries the database for the maximum date and constructs URLs for each
    file format. Attempts to access each URL and returns True if any file
    is found (HTTP 200 status code).
    
    Args:
        format (List[str]): A list of file format extensions to check.
        date (str): The date string to construct the filename for checking updates.
            2025-01: 202501 -> Result: terceirizados202501
            2025-05: maio -> Result: terceirizadosmaio
            2025-09: setembro -> Result: terceirizadossetembro
            2024-09: 202409 -> Result: terceirizados202409
            2024-05: 202405 -> Result: terceirizados202405
            2024-01: 202401 -> Result: terceirizados202401
            2023-09: 202309 -> Result: terceirizados202309
            2023-05: 202305 -> Result: terceirizados202305
            2023-01: 202301 -> Result: terceirizados202301
            2022-09: 202209 -> Result: terceirizados202209
            2022-05: 202205 -> Result: terceirizados202205
            2022-01: 202201 -> Result: terceirizados202201
            2021-09: 202109 -> Result: terceirizados202109
            2021-05: 202105 -> Result: terceirizados202105
            2021-01: _202101 -> Result: terceirizados_202101
            2020-09: _202009 -> Result: terceirizados_202009
            2020-05: _202005 -> Result: terceirizados_202005
            2020-05: _202001 -> Result: terceirizados_202001
            2019-09: _201909 -> Result: terceirizados_201909
            2019-05: _201905 -> Result: terceirizados_201905
            2019-05: -201901_1 -> Result: terceirizados-201901_1
    
    Returns:
        bool: True if at least one file is found at the remote URL,
              False otherwise.
    
    Raises:
        Logs a message if a URL is found and the pipeline is starting.
    """
    date_max_date = max_date_duckdb(file_parquet = "gs://br-cgu-terceirizados/terceirizados/*.parquet")
    
    build_filename = build_filenames(yyyymm = date_max_date)
    
    for formato in format:
        if date is None:
            for build in build_filename:
                filename = f'{build}{formato}'
                url = f'{constants.URL}{filename}'
                log(f'Checking for update at {url}...')
            
        elif date is not None:
            filename = f'{date}{formato}'
            url = f'{constants.URL}{filename}'
            log(f'Checking for update at {url}...')

        try:
            r = requests.get(
                url,
                cookies=constants.COOKIES,
                headers=constants.HEADERS,
                timeout=10
            )

            if r.status_code == 200:
                log(f'Arquivo encontrado: {url}')
                log(f'URL  exists: {url}!!! Starting pipeline...')
                os.makedirs('input', exist_ok=True)
                log(f'downloading in input/{date}{formato}...')
                with open(f'input/{date}{formato}', 'wb') as f:
                    f.write(r.content)
                return True

            elif r.status_code == 404:
                log(f'Não encontrado (404): {url}')
                continue

            else:
                log(f'Status inesperado {r.status_code}: {url}')

        except requests.exceptions.RequestException as e:
            log(f"Erro de conexão em {url}: {e}")
            return False

@task
def ingest_and_partition(input : str, output : str) -> None:
    """
    Ingest data from CSV or XLSX files and partition them by year and month.
    This function processes all files in the input directory, validates their
    schema against predefined constants, and converts them to Parquet format
    partitioned by 'Ano_Carga' (year) and 'Num_Mes_Carga' (month).
    Args:
        input (str): Path to the input directory containing CSV or XLSX files.
        output (str): Path to the output directory where partitioned Parquet files will be saved.
    Returns:
        None
    Raises:
        FileNotFoundError: If the input directory does not exist.
        KeyError: If required columns 'Ano_Carga' or 'Num_Mes_Carga' are missing when the schema is forced.
    Notes:
        - CSV files are expected to use ';' as separator and 'latin1' encoding.
        - If columns don't match constants.COLUMNS, the schema is forced using the predefined column names.
        - Output files are named with the pattern: terceirizados_{year}{month}.parquet
        - All data is read as string dtype to maintain data integrity.
    """
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
        
        for ano in df['Ano_Carga'].dropna().unique():
            for mes in df['Num_Mes_Carga'].dropna().unique():
                particionamento = f"ano={ano}/mes={mes}"
                log(f"Creating partition: {particionamento}...")
                os.makedirs(output, exist_ok=True)
                
                df.to_parquet(f"{output}/terceirizados_{ano}-{mes.zfill(2)}.parquet", index=False,)


@task
def download_all_data(url : str = 'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados'):
    """
    Download data files from a government website and save them locally.
    This task function fetches an HTML page, parses it to find all downloadable
    files (CSV and XLSX formats), and downloads them to a local 'input' directory.
    Files that already exist locally are skipped to avoid redundant downloads.
    Args:
        url (str): The URL of the government data portal to scrape.
                Defaults to 'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados'
    Returns:
        None
    Raises:
        requests.exceptions.HTTPError: If the HTTP request fails for any downloaded file.
    Notes:
        - Creates 'input' directory if it doesn't exist
        - Skips files that already exist locally
        - Uses predefined COOKIES and HEADERS for HTTP requests
        - Only downloads files ending in .csv or .xlsx
    """

    r = requests.get(url)

    soup = bs(r.text, 'html.parser')

    os.makedirs('input', exist_ok=True)

    for x in soup.find_all('a', class_="internal-link", href=True):
        if x['href'].endswith(('.csv', '.xlsx')):
            log(f"Downloading {x['href']}...")
            filename = x['href'].split('/')[-1]
            filepath = os.path.join('input', filename)
            if os.path.exists(filepath):
                log(f"{filename} already exists, skipping.")
                continue
            
            log(f"Saving to {filepath}...")

            response = requests.get(x['href'], cookies=constants.COOKIES, headers=constants.HEADERS)
            response.raise_for_status()

            with open(filepath, 'wb') as f:
                f.write(response.content)