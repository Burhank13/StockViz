
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date


def get_financial_report(ticker):

    # try:
    urlfinancials = 'https://www.marketwatch.com/investing/stock/'+ticker+'/financials'
    urlbalancesheet = 'https://www.marketwatch.com/investing/stock/' + \
        ticker+'/financials/balance-sheet'

    text_soup_financials = BeautifulSoup(
        requests.get(urlfinancials).text, "lxml")  # read in
    text_soup_balancesheet = BeautifulSoup(
        requests.get(urlbalancesheet).text, "lxml")  # read in

    # build lists for Income statement
    titlesfinancials = text_soup_financials.findAll(
        'td', attrs={'class': 'overflow__cell'})
    # print(titlesfinancials)
    epslist = []
    netincomelist = []
    longtermdebtlist = []
    interestexpenselist = []
    ebitdalist = []

    for title in titlesfinancials:
        if 'EPS (Basic)' in title.text:
            for td in title.findNextSiblings(attrs={'class': 'overflow__cell'}):
                epslist.append(td.text)
        if 'Net Income' in title.text:
            for td in title.findNextSiblings(attrs={'class': 'overflow__cell'}):
                netincomelist.append(td.text)
        if 'Interest Expense' in title.text:
            for td in title.findNextSiblings(attrs={'class': 'overflow__cell'}):
                interestexpenselist.append(td.text)
        if 'EBITDA' in title.text:
            for td in title.findNextSiblings(attrs={'class': 'overflow__cell'}):
                ebitdalist.append(td.text)

    # # find the table headers for the Balance sheet
    titlesbalancesheet = text_soup_balancesheet.findAll(
        'td', {'class': 'overflow__cell'})
    equitylist = []
    for title in titlesbalancesheet:
        if 'Total Shareholders\' Equity' in title.text:
            for td in title.findNextSiblings(attrs={'class': 'overflow__cell'}):
                equitylist.append(td.text)
        if 'Long-Term Debt' in title.text:
            for td in title.findNextSiblings(attrs={'class': 'overflow__cell'}):
                longtermdebtlist.append(td.text)

    # # get the data from the income statement lists
    # # use helper function get_element
    # eps = get_element(epslist, 0)
    # epsGrowth = get_element(epslist, 1)
    # netIncome = get_element(netincomelist, 0)
    # shareholderEquity = get_element(equitylist, 0)
    # roa = get_element(equitylist, 1)

    # longtermDebt = get_element(longtermdebtlist, 0)
    # interestExpense = get_element(interestexpenselist, 0)
    # ebitda = get_element(ebitdalist, 0)

    # load all the data into dataframe
    fin_df = pd.DataFrame({'Eps': epslist[:5], 'Net Income': netincomelist[:5], 'Shareholder Equity': equitylist[:5],
                           'Longterm Debt': longtermdebtlist[:5], 'Interest Expense': interestexpenselist[:5], 'Ebitda': ebitdalist[:5]},
                          index=range(date.today().year-5, date.today().year))
    fin_df.reset_index(inplace=True)
    return fin_df


def get_element(list, element):
    try:
        return list[element]
    except:
        return '-'
