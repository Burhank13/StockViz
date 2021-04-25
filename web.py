
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date

urlfinancials = 'https://www.marketwatch.com/investing/stock/msft/financials'
urlbalancesheet = 'https://www.marketwatch.com/investing/stock/msft/financials/balance-sheet'

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


def get_element(list, element):
    try:
        return list[element]
    except:
        return '-'


# # get the data from the income statement lists
# # use helper function get_element
# eps = get_element(epslist, 4)
# epsGrowth = get_element(epslist, 4)
# netIncome = get_element(netincomelist, 4)
# shareholderEquity = get_element(equitylist, 4)
# roa = get_element(equitylist, 4)

# longtermDebt = get_element(longtermdebtlist, 4)
# interestExpense = get_element(interestexpenselist, 4)
# ebitda = get_element(ebitdalist, 4)

# load all the data into dataframe
fin_df = pd.DataFrame({'eps': epslist[:5], 'net Income': netincomelist[:5], 'shareholder Equity': equitylist[:5],
                       'longterm Debt': longtermdebtlist[:5], 'interest Expense': interestexpenselist[:5], 'ebitda': ebitdalist[:5]},
                      index=range(date.today().year-5, date.today().year))

fin_df.reset_index(inplace=True)
print(fin_df)
# return fin_df


# def get_element(list, element):
#     try:
#         return list[element]
#     except:
#         return '-'

# content = driver.page_source
# soup = BeautifulSoup(content)
# for a in soup.findAll('a',href=True, attrs={'class':'_31qSD5'}):
#     name=a.find('div', attrs={'class':'_3wU53n'})
#     price=a.find('div', attrs={'class':'_1vC4OE _2rQ-NK'})
#     rating=a.find('div', attrs={'class':'hGSR34 _2beYZw'})
#     products.append(name.text)
#     prices.append(price.text)
#     ratings.append(rating.text)
# df = pd.DataFrame({'Product Name':products,'Price':prices,'Rating':ratings})
# df.to_csv('products.csv', index=False, encoding='utf-8')
