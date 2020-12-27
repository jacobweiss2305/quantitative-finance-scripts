# -*- coding: utf-8 -*-
"""
Created on Tue Jul 03 11:00:39 2018

@author: jweiss
"""

import os as os
import pandas.io.sql as pds
import pandas as pd
import imp
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import datetime
from lxml import html  
from time import sleep
from urllib import urlopen
from bs4 import BeautifulSoup
import time
import re


SQL =  """
select
top 10
	--th.tickerID,
	t.name
	--t.description,
	--th.avgDailyVolume_180,
	--th.[beta],
	--th.[localPrice]
from
	tickerhistory th
	inner join ticker t on th.tickerID = t.tickerid
where
	th.timeTrade > getdate() -1 and
	th.avgDailyVolume_180 is not null and
	figiSecurityType2 = 'Common Stock' and
	th.[beta] > 1.5 and
    name <> 'RDFN' 
    
order by
	th.avgDailyVolume_180 desc
"""
            
auth = imp.load_source('env', os.environ['PYENV']) 
cnxn = auth.db_cnxn_pyodbc
def run_query_with_output(query,index_column,conn):
    if index_column is None:
        data = pds.read_sql_query(query,conn)
    else:
        data = pds.read_sql_query(query,conn,index_col = pd.to_datetime(index_column))
    return data
data = run_query_with_output(SQL,None,cnxn)
cnxn.close()

#Scrape Pricing Data
tickers_list = [str(i) for i in data['name'].tolist()]
start = '01/01/2010'
end = '07/03/2018'

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

index1 = ['timetrade','open','High','Low','Close','Volume','adjClose']
date1 = str(int(time.mktime(datetime.datetime.strptime(start, "%m/%d/%Y").timetuple())))
date2 = str(int(time.mktime(datetime.datetime.strptime(end, "%m/%d/%Y").timetuple())))
priceUrl = 'https://finance.yahoo.com/quote/'
priceUrl2 = '/history?period1='
priceUrl3 = '&period2='
priceUrl4 = '&interval=1d&filter=history&frequency=1d'  
url_list = [str(priceUrl+i+priceUrl2+date1+priceUrl3+date2+priceUrl4) for i in tickers_list]
urlOpen_list = []
for i in url_list:
   urlOpen_list.append(urlopen(i))  
BeautifulSoup_List = [BeautifulSoup(i, 'html.parser') for i in urlOpen_list]
BeautifulSoup_List
Soup_list = [str(i) for i in BeautifulSoup_List]
text_start = "HistoricalPriceStore"
text_end = "isPending"
raw_data = []
for i in Soup_list:
    raw_data.append(i[i.find(text_start)+len(text_start):i.rfind(text_end)])  
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
    for k in range(len(clean_filter_pandas_list)):
        clean_filter_pandas_list[k]['adjClose'] = clean_filter_pandas_list[k]['adjClose'].map(lambda x: re.sub('[^0-9\.]+','',x))
        clean_filter_pandas_list[k]['timetrade'] = clean_filter_pandas_list[k]['timetrade'].apply(lambda z: datetime.datetime.fromtimestamp(int(z)).strftime('%Y-%m-%d'))

updated_list = [i[['adjClose','timetrade']] for i in clean_filter_pandas_list]
price_frame = [updated_list[i]['adjClose'].tolist() for i in range(len(updated_list))]
final_frame = pd.DataFrame(price_frame).T
final_frame.columns = tickers_list
final_frame['date'] = updated_list[0]['timetrade']
plot_frame = final_frame.sort_values(by='date', ascending=True)
plot_frame2 = plot_frame[tickers_list]
plt3 = plot_frame2.apply(pd.to_numeric, errors='coerce')


plt.figure(); plt3.plot(legend=False);


tickers_list





























