# -*- coding: utf-8 -*-
"""
Created on Fri May 11 15:42:54 2018

@author: jweiss
"""

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

tickers_df = pd.read_excel('C:/Users/jweiss/Desktop/SP500.xls')
tickers_list = tickers_df['Ticker'].tolist()
tickers = [str(i) for i in tickers_list]
start = '01/01/2018'
end = '05/09/2018'

def list_flatten(l, a=None):
    #check a
    if a is None:
        #initialize with empty list
        a = []

    for i in l:
        if isinstance(i, list):
            list_flatten(i, a)
        else:
            a.append(i)
    return a

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

raw_data = []
for i in Soup_list:
    raw_data.append(i[i.find(start)+len(start):i.rfind(end)])
    
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

len_data = []
pandas_list = []
for i in data_list:
    pandas_list.append(pd.DataFrame(i)) 
    clean_pandas_list = [len(pandas_list[z].columns) for z in range(len(pandas_list))] 
    tuple_pandas_list = [(pandas_list[k],clean_pandas_list[k]) for k in range(len(pandas_list))]
    filter_pandas_list = [x for (x,y) in tuple_pandas_list if y == 7]
    clean_filter_pandas_list = [filter_pandas_list[y].dropna() for y in range(len(filter_pandas_list))]
    for j in clean_filter_pandas_list:
        j.columns=index1
    df = pd.concat(clean_filter_pandas_list)
    df['timetrade'] = df['timetrade'].apply(lambda z: datetime.datetime.fromtimestamp(int(z)).strftime('%Y-%m-%d'))
    df['adjClose'].str.replace('}', '')

len_data = []
for j in range(len(clean_filter_pandas_list)):
    len_data.append(len(clean_filter_pandas_list[j]['adjClose']))
 
ticker_list = [[tickers[k]]*len_data[k] for k in range(len(clean_filter_pandas_list))]
df['filename'] = list_flatten(ticker_list)
