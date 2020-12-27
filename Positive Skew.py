# -*- coding: utf-8 -*-
"""
Created on Mon Apr 23 12:16:15 2018

@author: jweiss
"""

import pandas as pd
import numpy as np
import quandl
import datetime
import json
from finsymbols import symbols
import random


#------------------------------------------------------------------------------
#Scrape Symbols
sp500_JSON = symbols.get_sp500_symbols()
sp500_DF = pd.read_json(json.dumps(sp500_JSON))
sp500_list = list(sp500_DF['symbol'])
sp500 = sorted([str(i).replace('.','_') for i in sp500_list])
#------------------------------------------------------------------------------
start = '2012-01-01'
end = '2017-12-31'
startYear = datetime.datetime.strptime(start, '%Y-%m-%d').year
endYear = datetime.datetime.strptime(end, '%Y-%m-%d').year

#------------------------------------------------------------------------------
#SP500 Constituent Returns
sp500_test=sp500[0:250]

data1 = quandl.get_table('WIKI/PRICES', ticker = sp500_test, 
                        qopts = { 'columns': ['date', 'ticker','adj_close'] }, 
                        date = { 'gte': start, 'lte': end}, 
                        paginate=True)

sp500_test2=sp500[250:len(sp500)]
data2 = quandl.get_table('WIKI/PRICES', ticker = sp500_test2, 
                        qopts = { 'columns': ['date', 'ticker','adj_close'] }, 
                        date = { 'gte': start, 'lte': end}, 
                        paginate=True)

data = data1.merge(data2, how = 'outer')
data = data.dropna(how = 'any')
data['year'] = data.date.apply(lambda x: x.year)

returns = {}

for year in range(startYear, endYear):
    returns[year] = []
    for ticker in sp500:
        returnTicker = data[(data.ticker == ticker) & (data.year == year)].sort_values(by=['date'])
        if len(returnTicker) > 200:
            returnTicker = (1 + returnTicker.adj_close.pct_change()).cumprod() - 1
            returnTicker = returnTicker.iloc[-1]
            returns[year].append(returnTicker)

analysis = pd.DataFrame(columns = ['year', 'holdingsCount', 'medianReturn'])

for year in range(startYear, endYear):
    for i in range(1,len(returns[year])):
        portfolios = []
        for j in range(1, 500):
            portfolios.append(np.average(random.sample(returns[year], i)))
        analysis = analysis.append({'year': year, 'holdingsCount': i, 'medianReturn': np.median(portfolios)}, ignore_index=True)
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        