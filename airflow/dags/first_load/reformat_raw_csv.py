#  Daniel Lillard
#  2025.05.26
#  This file reformats the raw CSV's from yfinance into something that duckDB can parse.
#  below is a sample of what the file looks like and what it will be changed to.
# I know this could all be done in duckdb and the insert could be simpler but I want to break this down
# into as many parts as I can, I think that is airflow best practice.

#   Comes as follows:
#   Price	Close	High	Low	Open	Volume
#   Ticker	SO	SO	SO	SO	SO
#   Date					
#   1981-12-31	0.282410442829132	0.288294038879982	0.282410442829132	0.285352204115376	220985

#   We instead want:
#   Date	Close	High	Low	Open	Volume
#   1981-12-31	0.282410442829132	0.288294038879982	0.282410442829132	0.285352204115376	220985


import pandas as pd     # for manipulating the data.

df = pd.read_csv('C:\\file_struct\\documents\\programming\\data\\airflow_financial_project\\hist\\so.csv',skiprows=2)


df.columns = ['Date','Close','High','Low','Open','Volume']

df.to_csv('C:\\file_struct\\documents\\programming\\data\\airflow_financial_project\\hist\\processed_raw\\so_raw.csv')
