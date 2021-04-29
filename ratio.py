import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
from dash_utils import make_table, make_card, ticker_inputs, make_item


def ratios(ticker):
    ratios = 'https://www.marketwatch.com/investing/stock/' + \
        ticker+'/company-profile?mod=mw_quote_tab'
    ratio_financials = BeautifulSoup(
        requests.get(ratios).text, "lxml")

    titlesfinancials = ratio_financials.findAll(
        'td', attrs={'class': 'table__cell w75'})
    pe = ''
    pricetobook = ''
    equity = ''
    enterprise = ''
    currentratio = ''
    for title in titlesfinancials:
        if 'P/E Current' in title.text:
            for td in title.findNextSiblings(attrs={'class': 'table__cell w25'}):
                pe = td.text
        if 'Price to Book Ratio' in title.text:
            for td in title.findNextSiblings(attrs={'class': 'table__cell w25'}):
                pricetobook = td.text
        if 'Return on Equity' in title.text:
            for td in title.findNextSiblings(attrs={'class': 'table__cell w25'}):
                equity = td.text
        if 'Total Debt to Enterprise Value' in title.text:
            for td in title.findNextSiblings(attrs={'class': 'table__cell w25'}):
                enterprise = td.text
        if 'Current Ratio' in title.text:
            for td in title.findNextSiblings(attrs={'class': 'table__cell w25'}):
                currentratio = td.text
    cards1 = [dbc.Col(make_card("P/E Current ", "secondary", pe)), dbc.Col(make_card("Price to Book Ratio", "secondary", pricetobook)),
              dbc.Col(make_card("Total Debt to Enterprise Value", 'secondary', enterprise)), dbc.Col(
        make_card("Current Ratio", 'secondary', currentratio))]
    return cards1
