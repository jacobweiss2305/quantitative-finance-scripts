# -*- coding: utf-8 -*-
"""
Created on Mon May 07 11:05:48 2018

@author: jweiss
"""

#Yahoo Price to Database

from lxml import html  
from time import sleep
from urllib import urlopen
from bs4 import BeautifulSoup
import re
import time
import datetime
import pandas as pd
import imp
import os
import numpy as np

tickers_df = pd.read_excel('C:/Users/jweiss/Desktop/Index_constituents/SP500.xls')
tickers_DF_LIST = tickers_df['Ticker'].tolist()
tickers = [str(r) for r in tickers_DF_LIST]
start = '01/01/2014'
end = '05/09/2018'

auth = imp.load_source('env', os.environ['PYENV'])
index1 = ['timetrade','open','High','Low','Close','Volume','adjClose']
date1 = str(int(time.mktime(datetime.datetime.strptime(start, "%m/%d/%Y").timetuple())))
date2 = str(int(time.mktime(datetime.datetime.strptime(end, "%m/%d/%Y").timetuple())))
priceUrl = 'https://finance.yahoo.com/quote/'
priceUrl2 = '/history?period1='
priceUrl3 = '&period2='
priceUrl4 = '&interval=1d&filter=history&frequency=1d'  
url_list = [str(priceUrl+i+priceUrl2+date1+priceUrl3+date2+priceUrl4) for i in tickers]


urlOpen_list = []
for i in url_list:
   urlOpen_list.append(urlopen(i))  
BeautifulSoup_List = [BeautifulSoup(i, 'html.parser') for i in urlOpen_list]
Soup_list = [str(i) for i in BeautifulSoup_List]

start = "HistoricalPriceStore"
end = "isPending"

for i in Soup_list:
    raw_data = []
    raw_data.append(i[i.find(start)+len(start):i.rfind(end)])

for i in raw_data:
    parsed_data = []
    parsed_data.append(i[13:len(i)-3])

for i in parsed_data:
    clean_data = []
    clean_data.append(re.sub('[^0-9\.\}\,]+','',i))   

for i in clean_data:
    group_list = []
    group_list.append(i.split('},'))
    
for j in range(len(group_list)):
    data_list = []
    data_list.append([i.split(',') for i in group_list[j]])    

for i in data_list:
    len_data = []
    pandas_list = []
    pandas_list.append(pd.DataFrame(i)) 
    clean_pandas_list = [len(pandas_list[z].columns) for z in range(len(pandas_list))] 
    tuple_pandas_list = [(pandas_list[k],clean_pandas_list[k]) for k in range(len(pandas_list))]
    filter_pandas_list = [x for (x,y) in tuple_pandas_list if y == 7]
    clean_filter_pandas_list = [filter_pandas_list[y].dropna() for y in range(len(filter_pandas_list))]
    for j in clean_filter_pandas_list:
        j.columns=index1
    clean_filter_pandas_list[i]








adj_pandas_list = []
for j in range(len(pandas_list)):
    a = pandas_list[j].fillna(np.nan)
    b = a.dropna()
    c = b.drop(index1[1:6], axis=1)
    c['adj date'] = c['date'].apply(lambda z: datetime.datetime.fromtimestamp(int(z)).strftime('%m-%d-%Y'))
    d = c.drop(index1[0], axis=1)
    d = d[['adj date','adjclose']]
    d['adjclose'] = d['adjclose'].str.replace('}', '')
    d['adjclose'] = d['adjclose'].apply(lambda p: float(p))    
    e = d.iloc[::-1]
    e['pct change'] = e['adjclose'].pct_change(1)
    e['cumluative_return'] = (np.exp(np.log1p(e['pct change']).cumsum()))-1
    f = e.dropna()
    adj_pandas_list.append(f)    

cum_returns_list = [adj_pandas_list[i]['cumluative_return'] for i in range(len(adj_pandas_list))]
cum_returns_df = pd.DataFrame(cum_returns_list).T
cum_returns_df.columns = tickers
cum_returns_df['date'] = adj_pandas_list[0]['adj date']
cum_returns = cum_returns_df.dropna()
cum_returns = cum_returns.iloc[::-1]
cum_returns


