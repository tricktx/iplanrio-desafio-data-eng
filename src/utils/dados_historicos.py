import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import os
import openpyxl

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

def ingest_and_partition():
    for file in os.listdir('input'):
        print(f"Processing file: {file}...")

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
            print("Columns match, proceeding to read full dataset.")

            if file.endswith('.csv'):
                df = pd.read_csv(filepath, sep=';', encoding='latin1', dtype=str)

            else:
                df = pd.read_excel(filepath, dtype=str)

        else:
            print(f"Columns do not match for {file}, forcing schema...")

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

        print(f"Loaded {file} with shape {df.shape}")
        
        for ano in df['Ano_Carga'].unique():
            for mes in df['Num_Mes_Carga'].unique():
                particionamento = f"ano={ano}/mes={mes}"
                print(f"Creating partition: {particionamento}...")
                os.makedirs("output", exist_ok=True)
                
                df.to_parquet(f"output/terceirizados_{ano}{mes.zfill(2)}.parquet", index=False,)

def download_data():

    r = requests.get('https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados')

    soup = bs(r.text, 'html.parser')

    os.makedirs('input', exist_ok=True)

    for x in soup.find_all('a', class_="internal-link", href=True):
        if x['href'].endswith(('.csv', '.xlsx')):
            print(f"Downloading {x['href']}...")
            filename = x['href'].split('/')[-1]
            filepath = os.path.join('input', filename)
            if os.path.exists(filepath):
                print(f"{filename} already exists, skipping.")
                continue
            
            print(f"Saving to {filepath}...")

            response = requests.get(x['href'], cookies=COOKIES, headers=HEADERS)
            response.raise_for_status()

            with open(filepath, 'wb') as f:
                f.write(response.content)

if __name__ == "__main__":
    # download_data()
    ingest_and_partition()