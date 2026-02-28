import requests
import os
from src.pipelines.constants import constants
from dateutil.relativedelta import relativedelta
import duckdb

def max_date_duckdb():

    con = duckdb.connect('terceirizados-bronze.duckdb')

    result = con.execute("""SELECT max(max_date(
        CAST(Ano_Carga AS INT),
        CAST(Num_Mes_Carga AS INT),
        1
    ))
    FROM br_cgu_terceirizados.bronze""").fetchone()[0]

    total = str(result + relativedelta(months=4))[0:7].replace('-', '')
    
    return total


def download_data():
    for formato in ['.csv', '.xlsx']:
        url = f'{constants.URL}{max_date_duckdb}{formato}'
        
        try:
            r = requests.get(
                url,
                cookies=constants.COOKIES,
                headers=constants.HEADERS,
                timeout=10
            )

            if r.status_code == 200:
                print(f'Arquivo encontrado: {url}')
                break

            elif r.status_code == 404:
                print(f'Não encontrado (404): {url}')
                continue

            else:
                print(f'Status inesperado {r.status_code}: {url}')

        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão em {url}: {e}")
            return

    os.makedirs('input', exist_ok=True)
    print(f'Baixando arquivo para input/{max_date_duckdb}{formato}...')
    with open(f'input/{max_date_duckdb}{formato}', 'wb') as f:
        f.write(r.content)
