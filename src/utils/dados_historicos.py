import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import os
from prefect import task, flow
from src.utils.setup import upload_files_in_directory, log

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

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    # 'Cookie': 'lgpd-cookie-v2={"v":7,"g":[{"id":"cookies-estritamente-necessarios","on":true},{"id":"cookies-de-desempenho","on":true},{"id":"cookies-de-terceiros","on":true}]}; _fbp=fb.2.1762802006895.597212056308951418; _tt_enable_cookie=1; _ttp=01K9QJZZ6MDP9XY4WEQG4NNTBY_.tt.2; _gcl_au=1.1.2002285091.1770750083; Encrypted-Local-Storage-Key=N6u/NRWe2mEq9UMCpGHhdD3KBA68uCM5BQwuI1V+96Q; ttcsid=1771549106857::tLMQJAg2V-9Xfm0bZulc.47.1771549162814.0::1.55317.55750::5548.1.456.2256::1491.7.1600; ttcsid_CJVHHJ3C77U20ERJTS3G=1771549106857::PNN-CSJfXlvKxeK2swm4.15.1771549162814.1',
}

COOKIES = {
    'lgpd-cookie-v2': '{"v":7,"g":[{"id":"cookies-estritamente-necessarios","on":true},{"id":"cookies-de-desempenho","on":true},{"id":"cookies-de-terceiros","on":true}]}',
    '_fbp': 'fb.2.1762802006895.597212056308951418',
    '_tt_enable_cookie': '1',
    '_ttp': '01K9QJZZ6MDP9XY4WEQG4NNTBY_.tt.2',
    '_gcl_au': '1.1.2002285091.1770750083',
    'Encrypted-Local-Storage-Key': 'N6u/NRWe2mEq9UMCpGHhdD3KBA68uCM5BQwuI1V+96Q',
    'ttcsid': '1771549106857::tLMQJAg2V-9Xfm0bZulc.47.1771549162814.0::1.55317.55750::5548.1.456.2256::1491.7.1600',
    'ttcsid_CJVHHJ3C77U20ERJTS3G': '1771549106857::PNN-CSJfXlvKxeK2swm4.15.1771549162814.1',
}


    
@task
def ingest_and_partition():
    """
    Ingest and partition historical data from CSV or XLSX files.
    This task function processes input files (CSV or XLSX format) and performs the following operations:
    1. Reads the header of each file to extract column names
    2. Validates columns against a predefined COLUMNS constant
    3. If columns don't match, forces the schema using COLUMNS as column names
    4. Loads the full dataset with string dtype
    5. Partitions the data by year (Ano_Carga) and month (Num_Mes_Carga)
    6. Exports each partition to a parquet file in the output directory
    Supported file formats:
        - CSV: Expects semicolon separator and latin1 encoding
        - XLSX: Expects default Excel format
    File handling:
        - CSV: Uses latin1 encoding when columns match, utf-8 when forcing schema
        - XLSX: Consistent handling for both matching and mismatched schemas
    Output:
        Parquet files saved to 'output/' directory with naming convention:
        'terceirizados_{ano}{mes_zero_padded}.parquet'
    Raises:
        FileNotFoundError: If input directory doesn't exist
        ValueError: If required columns (Ano_Carga, Num_Mes_Carga) are missing after schema enforcement
    Notes:
        - All data is read as strings (dtype=str)
        - Uses Prefect @task decorator for workflow orchestration
        - Logging is performed at key processing steps
    """
    for file in os.listdir('input'):
        log(f"Processing file: {file}...")

        filepath = os.path.join('input', file)

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


        if cols == COLUMNS:
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

        log(f"Loaded {file} with shape {df.shape}")
        
        for ano in df['Ano_Carga'].unique():
            for mes in df['Num_Mes_Carga'].dropna().unique():
                particionamento = f"ano={ano}/mes={mes}"
                log(f"Creating partition: {particionamento}...")
                os.makedirs("output", exist_ok=True)
                
                df.to_parquet(f"output/terceirizados_{ano}-{mes.zfill(2)}.parquet", index=False,)

@task
def download_data(url : str = 'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados'):
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

            response = requests.get(x['href'], cookies=COOKIES, headers=HEADERS)
            response.raise_for_status()

            with open(filepath, 'wb') as f:
                f.write(response.content)
                
@flow(
    log_prints=False,
    name="Dados Históricos"
)
def dados_historicos():
    ingest_and_partition()
    download_data()
    upload_files_in_directory(data_path_local = "output", destination_directory=["terceirizados/"])

