# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 10:09:58 2018

@author: jweiss
"""
#sp500 Pricing data
import pandas as pd
import quandl
import imp
import os
import datetime
import json
from pandas_datareader import data
import pandas_montecarlo
from finsymbols import symbols
from pandas import ExcelWriter
sp500_JSON = symbols.get_sp500_symbols()
sp500_DF = pd.read_json(json.dumps(sp500_JSON))
sp500_list = list(sp500_DF['symbol'])
sp500 = [str(i) for i in sp500_list]
df = data.get_data_yahoo("SPY")
df['return'] = df['Adj Close'].pct_change().fillna(0)
sp500_test=sp500[0:10]
store = list()
for ticker in sp500_test:
    df = data.get_data_yahoo(ticker)
    df['return'] = df['Adj Close'].pct_change().fillna(0)
    store.append(df['Adj Close'].as_matrix())
Returns = pd.DataFrame(store).T
Returns.columns = sp500_test
Returns.head()



























