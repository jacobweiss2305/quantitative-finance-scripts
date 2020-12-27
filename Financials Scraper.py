# -*- coding: utf-8 -*-
"""
Created on Mon May 14 11:52:06 2018

@author: jweiss
"""
from urllib import urlopen
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
import json


tickers = ['FDS','AAPL']

income_statement = ['https://finance.yahoo.com/quote/','/financials?p=']
balance_sheet = ['https://finance.yahoo.com/quote/','/balance-sheet?p=']
statement_of_cashflow = ['https://finance.yahoo.com/quote/','/cash-flow?p=']

url_list_IS = [str(income_statement[0]+i+income_statement[1]+i) for i in tickers]
url_list_CF = [str(statement_of_cashflow[0]+i+statement_of_cashflow[1]+i) for i in tickers]


#----------Balance Sheet------------------------------------------------------#
url_list_BS = [str(balance_sheet[0]+i+balance_sheet[1]+i) for i in tickers]
for i in url_list_BS:
   urlOpen_list = []
   urlOpen_list.append(urlopen(i))  
BeautifulSoup_List = [BeautifulSoup(i, "lxml") for i in urlOpen_list]
Soup_list = [str(i) for i in BeautifulSoup_List]
start = 'QuoteSummaryStore":'
end = ',"quoteType":{"exchange":'
for i in Soup_list:
    raw_data = []
    raw_data.append(i[i.find(start)+len(start):i.rfind(end)])
raw_data[0]
re.split('},"',raw_data[0])
