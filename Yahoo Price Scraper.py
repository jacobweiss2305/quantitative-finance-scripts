# -*- coding: utf-8 -*-
"""
Created on Fri Apr 20 12:11:41 2018

@author: jweiss
"""

#Yahoo Price Scraper

from lxml import html  
import requests
from time import sleep
import json
import argparse
from collections import OrderedDict
from time import sleep
from urllib import urlopen
from bs4 import BeautifulSoup
import re
import pandas as pd
from finsymbols import symbols
import random
import time
import datetime


def get_prices(tickers, start, end):
    index = ['date','open','high','low','close','volume','adjclose']
    date1 = str(int(time.mktime(datetime.datetime.strptime(start, "%m/%d/%Y").timetuple())))
    date2 = str(int(time.mktime(datetime.datetime.strptime(end, "%m/%d/%Y").timetuple())))
    priceUrl = 'https://finance.yahoo.com/quote/'
    priceUrl2 = '/history?period1='
    priceUrl3 = '&period2='
    priceUrl4 = '&interval=1d&filter=history&frequency=1d'  
    url_list = [str(priceUrl+i+priceUrl2+date1+priceUrl3+date2+priceUrl4) for i in tickers]
    #start_time = time.time()
    urlOpen_list = []
    for i in url_list:
       urlOpen_list.append(urlopen(i))
       #time.sleep(random.choice(range(1,30)))
    BeautifulSoup_List = [BeautifulSoup(i, 'html.parser') for i in urlOpen_list]
    Soup_list = [str(i) for i in BeautifulSoup_List]
    start = "HistoricalPriceStore"
    end = "isPending"
    raw_data = []
    for i in Soup_list:
        raw_data.append(i[i.find(start)+len(start):i.rfind(end)])
    parsed_data = []
    for i in raw_data:
        parsed_data.append(i[13:len(i)-2])
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
    adj_close = []
    for j in range(len(pandas_list)):
        adj_close.append(pandas_list[j]['adjclose'])      
    adj_df = pd.DataFrame([adj_close[i].str.replace('}', '') for i in range(len(tickers))])  
    dl = adj_df.values.tolist()
    ml = []
    for k in range(len(dl)):
        ml.append(dl[k][::-1])
    df = pd.DataFrame(ml).T
    df.columns = tickers
    return df

ticker_list = ['NKTN.L']
beg = '10/21/2017'
end = '7/01/2018'



get_prices(ticker_list, beg, end)





#soup = BeautifulSoup(pricePage, 'html.parser')
#raw_data = str(soup)
#start = "HistoricalPriceStore"
#end = "isPending"
#index = ['date','open','high','low','close','volume','adjclose']
#parse = raw_data[raw_data.find(start)+len(start):raw_data.rfind(end)]
#parse_list = parse[13:len(parse)-2]
#parsed = re.sub('[^0-9\.\}\,]+','', parse_list)
#parsed_list = parsed.split('},')
#data_frame = pd.DataFrame([i.split(",") for i in parsed_list])
#data_frame.columns = index
#data_frame
#sp500_JSON = symbols.get_sp500_symbols()
#sp500_DF = pd.read_json(json.dumps(sp500_JSON))
#sp500_list = list(sp500_DF['symbol'])
#sp500 = sorted([str(i).replace('.','_') for i in sp500_list])











 
        