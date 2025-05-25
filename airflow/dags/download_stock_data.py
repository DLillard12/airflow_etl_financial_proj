# Daniel Lillard
# 2025.05.12
# This file imports S&P500 stock data from the ytfinance package in python
# this is directly from the kaggle notebook: https://www.kaggle.com/code/jacksoncrow/download-nasdaq-historical-data
# the data is saved to the folder ..\\hist

import pandas as pd
import yfinance as yf
import os, contextlib
import lxml
import datetime

offset = 0
limit = 3000
period = '1d' # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max

directory_name = '/opt/airflow/hist/'+ str(datetime.date.today())
# Create the directory
try:
    os.mkdir(directory_name)
    print(f"Directory '{directory_name}' created successfully.")
except FileExistsError:
    print(f"Directory '{directory_name}' already exists.")
except PermissionError:
    print(f"Permission denied: Unable to create '{directory_name}'.")
except Exception as e:
    print(f"An error occurred: {e}")

# List of S&P 500 tickers
sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
symbols = sp500['Symbol'].tolist()

# add on the S and P 500
symbols.append('SPY')

limit = limit if limit else len(symbols)
end = min(offset + limit, len(symbols))
is_valid = [False] * len(symbols)
# force silencing of verbose API
with open(os.devnull, 'w') as devnull:
    with contextlib.redirect_stdout(devnull):
        for i in range(offset, end):
            s = symbols[i]
            data = yf.download(s, period=period)
            if len(data.index) == 0:
                continue
        
            is_valid[i] = True
            data.to_csv('/opt/airflow/hist/'+ str(datetime.date.today())+'/{}.csv'.format(s))

print('Total number of valid symbols downloaded = {}'.format(sum(is_valid)))