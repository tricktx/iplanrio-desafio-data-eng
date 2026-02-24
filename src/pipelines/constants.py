
class constants:
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
    
    URL = 'https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/terceirizados'