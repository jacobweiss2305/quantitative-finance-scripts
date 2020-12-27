# -*- coding: utf-8 -*-
"""
Created on Wed May 16 12:30:31 2018

@author: jweiss
"""

#Get Index Constituent Data
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

#load in constituent data
sp500 = pd.read_excel('C:/Users/jweiss/Desktop/Index_constituents/SP500.xls')
sp500_tickers = [str(r) for r in sp500['Ticker'].tolist()]
sp500_tickers1 = sp500_tickers[0:249]
sp500_tickers2 = sp500_tickers[250:len(sp500_tickers)]

R3000 = pd.read_excel('C:/Users/jweiss/Desktop/Index_constituents/R3000.xls')
R3000_tickers = [str(r) for r in R3000['Ticker'].tolist()]

DAX30 = pd.read_excel('C:/Users/jweiss/Desktop/Index_constituents/DAX30.xls')
DAX30_tickers = [str(r) for r in DAX30['Ticker'].tolist()]






















