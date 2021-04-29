import requests
import pandas as pd

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}


def get_screener(version):
    screen = requests.get(
        f'https://finviz.com/screener.ashx?v=151&f=fa_pe_profitable,fa_roe_o50,idx_sp500&ft=4&o=-roe', headers=headers).text

    tables = pd.read_html(screen)
    tables = tables[-2]
    tables.columns = tables.iloc[0]
    tables = tables[1:]
    return tables
