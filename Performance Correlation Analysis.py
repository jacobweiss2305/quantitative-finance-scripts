# -*- coding: utf-8 -*-
"""
Created on Tue May 08 10:31:03 2018

@author: jweiss
"""

#Performance Correlation Analysis
from lxml import html  
from time import sleep
from urllib import urlopen
from bs4 import BeautifulSoup
import re
import pandas as pd
import datetime
import numpy as np
import matplotlib.pyplot as plt; plt.rcdefaults()
import matplotlib.pyplot as plt
import pandas_datareader as web
import json
from finsymbols import symbols


#Scrape Symbols

tickers=['IT','FDS']
start = '05/01/2014'
end = '05/01/2018'

#URL
#-----------------------------------------------------------------------------#
index = ['date','open','high','low','close','volume','adjclose']
date1 = str(int(time.mktime(datetime.datetime.strptime(start, "%m/%d/%Y").timetuple())))
date2 = str(int(time.mktime(datetime.datetime.strptime(end, "%m/%d/%Y").timetuple())))
priceUrl = 'https://finance.yahoo.com/quote/'
priceUrl2 = '/history?period1='
priceUrl3 = '&period2='
priceUrl4 = '&interval=1d&filter=history&frequency=1d'  
url_list = [str(priceUrl+i+priceUrl2+date1+priceUrl3+date2+priceUrl4) for i in tickers]


#Get data from internet  
#-----------------------------------------------------------------------------#
urlOpen_list = []
for i in url_list:
   urlOpen_list.append(urlopen(i))
BeautifulSoup_List = [BeautifulSoup(i, 'html.parser') for i in urlOpen_list]
Soup_list = [str(i) for i in BeautifulSoup_List]
start_str = "HistoricalPriceStore"
end_str = "isPending"
raw_data = []
for i in Soup_list:
    raw_data.append(i[i.find(start_str)+len(start_str):i.rfind(end_str)])
    
    
#Clean data   
#-----------------------------------------------------------------------------#    
parsed_data = []
for i in raw_data:
    parsed_data.append(i[13:len(i)-3])    
clean_data = []
for i in parsed_data:
    clean_data.append(re.sub('[^0-9\.\}\,]+','',i))   
group_list = []
for i in clean_data:
    group_list.append(i.split('},'))
data_list = []
for j in range(len(group_list)):
    data_list.append([i.split(',') for i in group_list[j]])
parsed_data

pandas_list = []
for i in data_list:
    pandas_list.append(pd.DataFrame(i))
    for j in pandas_list:
        j.columns=index  
adj_pandas_list = []
for j in range(len(pandas_list)):
    a = pandas_list[j].fillna(np.nan)
    b = a.dropna()
    c = b.drop(index[1:6], axis=1)
    c['adj date'] = c['date'].apply(lambda z: datetime.datetime.fromtimestamp(int(z)).strftime('%m-%d-%Y'))
    d = c.drop(index[0], axis=1)
    d = d[['adj date','adjclose']]
    d['adjclose'] = d['adjclose'].str.replace('}', '')
    d['adjclose'] = d['adjclose'].apply(lambda p: float(p))    
    e = d.iloc[::-1]
    e['pct change'] = e['adjclose'].pct_change(1)
    e['cumluative_return'] = (np.exp(np.log1p(e['pct change']).cumsum()))-1
    f = e.dropna()
    adj_pandas_list.append(f)

#Cumulative Returns   
#-----------------------------------------------------------------------------#
cum_returns_list = [adj_pandas_list[i]['cumluative_return'] for i in range(len(adj_pandas_list))]
cum_returns_df = pd.DataFrame(cum_returns_list).T
cum_returns_df.columns = tickers
cum_returns_df['date'] = adj_pandas_list[0]['adj date']
cum_returns = cum_returns_df.dropna()
cum_returns = cum_returns.iloc[::-1]
cum_returns

#Correlation Matrix
#-----------------------------------------------------------------------------#
cum_returns.corr()
cum_returns.plot()
        
#Dispersion Analysis
#-----------------------------------------------------------------------------# 
#cum_returns['Average Return'] = cum_returns[tickers[1:len(tickers)]].mean(axis=1)
#disp_df = disp_df.drop(['date'], axis=1)
disp = cum_returns[tickers].apply(lambda x: x - cum_returns[tickers[0]])
dispersion = disp[tickers[1:len(tickers)]]
dispersion.plot()

avgs_disp = dict(zip(tickers,dispersion.mean(axis=0).tolist()))
sd_disp = dict(zip(tickers,dispersion.std(axis=0).tolist()))














