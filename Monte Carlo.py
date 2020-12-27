# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 08:55:20 2018

@author: jweiss
"""

#Random Selection of Constituent returns of the SP 500
import os
import pandas as pd
import numpy as np
import quandl
import imp
import os
import datetime
import json
import pandas_datareader as web
import pandas_montecarlo
from finsymbols import symbols
import random
import matplotlib.pyplot as plt
%matplotlib inline
import quandl
#quandl.ApiConfig.api_key = "YbsGkVvwF-nJbuf7cbC4"
quandl.ApiConfig.api_key = "natezkiWArzE7_mz-oc5"
#------------------------------------------------------------------------------
#Scrape Symbols
sp500_JSON = symbols.get_sp500_symbols()
sp500_DF = pd.read_json(json.dumps(sp500_JSON))
sp500_list = list(sp500_DF['symbol'])
sp500 = sorted([str(i).replace('.','_') for i in sp500_list])
#------------------------------------------------------------------------------
start='2012-01-01'
end= '2018-04-13'

#------------------------------------------------------------------------------
#SP500 Constituent Returns
sp500_test=sp500[0:250]


data1 = quandl.get_table('WIKI/PRICES', ticker = sp500_test, 
                        qopts = { 'columns': ['ticker','adj_close'] }, 
                        date = { 'gte': start, 'lte': end}, 
                        paginate=True)

sp500_test2=sp500[250:len(sp500)]
data2 = quandl.get_table('WIKI/PRICES', ticker = sp500_test2, 
                        qopts = { 'columns': ['ticker','adj_close'] }, 
                        date = { 'gte': start, 'lte': end}, 
                        paginate=True)
#check for NA
def check_na(df1,df2):
    if df1.isnull().values.any() == True:
            print('DF1 FAILED')
    elif df2.isnull().values.any() == True:
            print('DF2 FAILED')
    else:
        print('Pass')
check_na(data1,data2)
data = data1.merge(data2, how = 'outer')
data.head()
#------------------------------------------------------------------------------
#Export to excel
df = pd.DataFrame(sp500)
writer = pd.ExcelWriter('C:/Users/jweiss/Desktop/New folder/ticker_list.xlsx', engine='xlsxwriter')
df.to_excel(writer, sheet_name='pricing_data')
writer.save()
#------------------------------------------------------------------------------
#Load data from local
#cwd = os.getcwd()
#cwd = 'C:\\Users\\jweiss'
os.chdir("C:\Users\jweiss\Desktop\New folder")
#os.listdir('.')
xls = pd.ExcelFile('pricing_data.xlsx')
data = xls.parse('pricing_data')
data.head()

#------------------------------------------------------------------------------
#Calculate Benchmark Returns
bm_data= web.get_data_yahoo('SPY', start, end, interval='d')
bm_ret = ((bm_data['Adj Close'][::len(bm_data['Adj Close'])-1].tolist()[1]-bm_data['Adj Close'][::len(bm_data['Adj Close'])-1].tolist()[0])/bm_data['Adj Close'][::len(bm_data['Adj Close'])-1].tolist()[0])

bm_data['Adj Close'][::len(bm_data['Adj Close'])-1].tolist()[1]
bm_data['Adj Close'][::len(bm_data['Adj Close'])-1].tolist()[0]
bm_ret

#Unpack data and calculate returns
#------------------------------------------------------------------------------
stores = []
for i in sp500:
    stores.append(data[data['ticker'] == i])
stores[0]

price = []
for i in range(len(sp500)):
    price.append(stores[i]['adj_close'])
price[0]

ret = []    
for i in range(len(sp500)):
    ret.append(((price[i][::len(price[i])-1].tolist()[1]-price[i][::len(price[i])-1].tolist()[0])/price[i][::len(price[i])-1].tolist()[0]))



#Random Selection
#------------------------------------------------------------------------------
tot_z_score = []
means = []
median_list = []
stdev = []
binary  = []

for x in range(1,len(sp500)+1):
    draw = 1000
    draw_list = []
    for i in range(draw):
        Sample = random.sample(ret,x)
        weight = (1.0/float(x))
        weight_adj_returns = [i*weight for i in Sample]
        Total_returns = round(sum(weight_adj_returns),2)
        draw_list.append(Total_returns)
        med = np.median(draw_list)
        median_list.append(med)
        med = np.average(draw_list)
        means.append(med)

mean = np.average(draw_list)
    medians = np.median(draw_list)
    std = np.std(draw_list)
    means.append(mean)
    median_list.append(median)

plt.hist(tot_z_score, bins=50, alpha=0.5)    
plt.axvline(bm_ret, color='k', linestyle='dashed', linewidth=1)
plt.hist(draw_list, bins=50, alpha=0.5)    
plt.axvline(bm_ret, color='k', linestyle='dashed', linewidth=1)
np.average(draw_list)
plt.hist(draw_list, bins=50, alpha=0.5)    
plt.axvline(bm_ret, color='k', linestyle='dashed', linewidth=1)



#auth = imp.load_source('env', os.environ['PYENV'])
#sp500.to_sql('sAndPConstituents', auth.db_cnxn_sqlalchemy, if_exists = 'replace', index = False)
#yahoo_financials_sp500= YahooFinancials(sp500_list)
#sp500_cash_flow_data_an = yahoo_financials_sp500.get_financial_stmts('annual', 'cash')
#sp500_market_cap = yahoo_financials_sp500.get_market_cap()