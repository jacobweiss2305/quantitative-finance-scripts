# -*- coding: utf-8 -*-
"""
Created on Wed Aug 01 13:38:12 2018

@author: jweiss
"""

#Compare Pricing
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
import sqlalchemy as sa

tickers_list = ['9948.T','7250.T']

start = '01/01/2016'
end = '08/01/2018'

def list_flatten(l, a=None):
    if a is None:
        a = []
    for i in l:
        if isinstance(i, list):
            list_flatten(i, a)
        else:
            a.append(i)
    return a

#Scrape Data
columns = ['timetrade','open','High','Low','Close','Volume','adjClose']
url_list = [str('https://finance.yahoo.com/quote/'+i+'/history?period1='
            + str(int(time.mktime(datetime.datetime.strptime(start, "%m/%d/%Y").timetuple())))
            +'&period2='+str(int(time.mktime(datetime.datetime.strptime(end, "%m/%d/%Y").timetuple())))
            +'&interval=1d&filter=history&frequency=1d' ) for i in tickers_list]
BeautifulSoup_List = [str(BeautifulSoup(i, 'html.parser')) for i in [urlopen(i) for i in url_list]]

#Clean Date
text_start = "HistoricalPriceStore"
text_end = "isPending"
raw_data = [i[i.find(text_start)+len(text_start):i.rfind(text_end)][13:len(i)-3] for i in BeautifulSoup_List]
pandas_list = [pd.DataFrame([[i.split(',') for i in [re.sub('[^0-9\.\}\,]+','',i).split('},') 
                for i in raw_data][j] if len(i) > 1] for j in range(len([re.sub('[^0-9\.\}\,]+','',i).split('},') 
                for i in raw_data]))][k]) for k in range(len(raw_data))]
for i in range(len(pandas_list)):
    pandas_list[i].columns = columns
    pandas_list[i]['timetrade'] = pandas_list[i]['timetrade'].apply(lambda z: datetime.datetime.fromtimestamp(int(z)).strftime('%Y-%m-%d'))
    pandas_list[i] = pandas_list[i][['timetrade','adjClose']]
    #pandas_list[i]['adjClose'].apply(lambda z: int(z))
pandas_list[0]


# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('C:/Users/jweiss/Desktop/test.xlsx', engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object.
pandas_list[0].to_excel(writer, sheet_name='returns')

# Close the Pandas Excel writer and output the Excel file.
writer.save()






data.head()












































