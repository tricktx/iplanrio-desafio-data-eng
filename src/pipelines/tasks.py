import pandas as pd
import requests
import os
from src.pipelines.constants import constants
from src.utils.setup import max_date_duckdb, log
from prefect import task
from typing import List


@task
def check_for_update(format: List[str]) -> bool:
    """
    Check if updated data files are available at the remote URL.
    
    Queries the database for the maximum date and constructs URLs for each
    file format. Attempts to access each URL and returns True if any file
    is found (HTTP 200 status code).
    
    Args:
        format (List[str]): A list of file format extensions to check.
    
    Returns:
        bool: True if at least one file is found at the remote URL,
              False otherwise.
    
    Raises:
        Logs a message if a URL is found and the pipeline is starting.
    """
    date = max_date_duckdb()
    for formato in format:

        filename = f'{date}{formato}'
        url = f'{constants.URL}{filename}{formato}'
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
    def download_data(format: List[str] = ['.csv', '.xlsx']):
        """
        Download data files from a remote server in specified formats.
        Attempts to download files for the maximum date available in the database,
        trying each format in the provided list until a successful download (HTTP 200)
        is obtained. The file is saved to the 'input' directory.
        Args:
            format (List[str], optional): List of file format extensions to attempt.
                Defaults to ['.csv', '.xlsx'].
        Returns:
            None
        Raises:
            RequestException: Caught internally and logged; function returns early
                if a connection error occurs.
        Behavior:
            - Retrieves the maximum date from DuckDB
            - Iterates through each format attempting to fetch the file
            - Logs the result of each HTTP request (success, 404, or other status codes)
            - Stops iteration upon successful retrieval (HTTP 200)
            - Creates 'input' directory if it doesn't exist
            - Writes the downloaded file content to 'input/202501{format}'
            - Returns early if a connection error is encountered during any request
        Note:
            Uses constants for URL base, cookies, and headers from the constants module.
        """
    date = max_date_duckdb()
    for formato in format:
        filename = f'{date}{formato}'
        url = f'{constants.URL}{filename}{formato}'
        
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
    """
    Ingest data from CSV or XLSX files and partition them by year and month.
    This function processes all files in the input directory, validates their
    schema against predefined constants, and converts them to Parquet format
    partitioned by 'Ano_Carga' (year) and 'Num_Mes_Carga' (month).
    Args:
        input (str): Path to the input directory containing CSV or XLSX files.
        output (str): Path to the output directory where partitioned Parquet files
                     will be saved.
    Returns:
        None
    Raises:
        FileNotFoundError: If the input directory does not exist.
        KeyError: If required columns 'Ano_Carga' or 'Num_Mes_Carga' are missing
                 when the schema is forced.
    Notes:
        - CSV files are expected to use ';' as separator and 'latin1' encoding.
        - If columns don't match constants.COLUMNS, the schema is forced using
          the predefined column names.
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
        
        for ano in df['Ano_Carga'].unique():
            for mes in df['Num_Mes_Carga'].unique():
                particionamento = f"ano={ano}/mes={mes}"
                log(f"Creating partition: {particionamento}...")
                os.makedirs(output, exist_ok=True)
                
                df.to_parquet(f"{output}/terceirizados_{ano}{mes.zfill(2)}.parquet", index=False,)