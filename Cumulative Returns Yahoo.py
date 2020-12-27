# -*- coding: utf-8 -*-
"""
Created on Tue May 08 10:31:03 2018

@author: jweiss
"""

#Cumulative returns

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

#Get Tickers
#-----------------------------------------------------------------------------#
sp500 = pd.read_excel('C:/Users/jweiss/Desktop/Index_constituents/SP500.xls')
sp500_tickers = [str(r) for r in sp500['Ticker'].tolist()]
sp500_tickers1 = sp500_tickers[0:250]
sp500_tickers2 = sp500_tickers[251:len(sp500_tickers)]


R3000 = pd.read_excel('C:/Users/jweiss/Desktop/Index_constituents/R3000.xls')
R3000_tickers = [str(r) for r in R3000['Ticker'].tolist()]

DAX30 = pd.read_excel('C:/Users/jweiss/Desktop/Index_constituents/DAX30.xls')
DAX30_tickers = [str(r) for r in DAX30['Ticker'].tolist()]

misc = pd.read_excel('C:/Users/jweiss/Desktop/Index_constituents/MISC.xls')
misc_tickers = [str(r) for r in misc['Ticker'].tolist()]


tickers = ['REGI']
start = '01/01/2018'
end = '05/20/2018'

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
    c['date'] = c['date'].apply(lambda z: datetime.datetime.fromtimestamp(int(z)).strftime('%m-%d-%Y'))
    c['adjclose'] = c['adjclose'].map(lambda x: re.sub('[^0-9\.]+','',x))
    d = c.copy()
    d['adjclose'] = d['adjclose'].apply(lambda p: float(p))
    e = d.iloc[::-1]
    adj_pandas_list.append(e)
tr = (adj_pandas_list[0]['adjclose'].iloc[-1]/adj_pandas_list[0]['adjclose'].iloc[0])-1
tr






#Cumulative Returns   
#-----------------------------------------------------------------------------#
cum_returns_list = [adj_pandas_list[i]['cumluative_return'] for i in range(len(adj_pandas_list))]
cum_returns_df = pd.DataFrame(cum_returns_list).T
cum_returns_df.columns = tickers
cum_returns_df['date'] = adj_pandas_list[0]['adj date']
cum_returns = cum_returns_df.dropna()
cum_returns = cum_returns.iloc[::-1]





# Create a Pandas Excel writer using XlsxWriter as the engine.










