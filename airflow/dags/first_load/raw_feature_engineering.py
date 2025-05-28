# Daniel Lillard
# 2025.05.27
# This file takes the data from reformat_raw_csv.py and adds the engineered features as follows:
# Beta, MA5, MA10, Return and Volatility
# Also smooth the data.


# Steps:
# load data
# add the calculatable features
# add the beta
# add the clusters
# save to csv
# (outside of this) save to duckdb.

import pandas as pd

df = pd.read_csv('C:\\file_struct\\documents\\programming\\data\\airflow_financial_project\\hist\\processed_raw\\so_raw.csv',index_col=0)

from sklearn.linear_model import LinearRegression   # using this to determine the beta of stocks
import numpy as np


# Add the market return, from the S&P500.
df_s_and_p_500 = pd.read_csv('C:\\file_struct\\documents\\programming\\data\\airflow_financial_project\\hist\\processed_raw\\SPY_raw.csv', parse_dates=['Date'])
df_s_and_p_500['MarketReturn'] = df_s_and_p_500['Close'].pct_change()

print('s and p datatypes:', df_s_and_p_500.dtypes)

df['Date'] = df['Date'].astype('datetime64[ns]')

print('so datatypes:', df.dtypes)

# Merge market return into combined_df
df = pd.merge(df,
    df_s_and_p_500[['Date', 'MarketReturn']], 
    on='Date', how='left')



df["MA5"] = df["Close"].rolling(window=5).mean()
df["MA10"] = df["Close"].rolling(window=10).mean()
df["Return"] = df["Close"].pct_change(fill_method=None)
df["Volatility"] = df["Return"].rolling(window=5).std()
df["Close_tomorrow"] = df["Close"].shift(-1)
    
# Drop NaNs needed for regression
temp = df.dropna(subset=["Return", "MarketReturn"])
    
if len(temp) > 1:
    X = temp["MarketReturn"].values.reshape(-1, 1)
    y = temp["Return"].values
    model = LinearRegression().fit(X, y)
    beta = model.coef_[0]
else:
    beta = np.nan

df["Beta"] = beta

# Drop rows with any missing values
df.dropna(inplace=True)
df.reset_index(drop=True, inplace=True)

# Check output
print(df.head())