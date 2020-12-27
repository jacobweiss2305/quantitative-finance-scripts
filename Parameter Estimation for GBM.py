# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 15:22:44 2018

@author: jweiss
"""

#parameter esimtation of GBM
import pandas as pd
import numpy as np
from numpy.random import normal
import imp

#data
fun = imp.load_source('env', "C:\\Users\\jweiss\\Desktop\\Python Codes\\my_functions.py")
tickers_list = ['AAPL']
start = '01/01/2016'
end = '08/01/2018' 
data = fun.priceScraper(tickers_list, start, end)[0]

#returns
r = np.log(data['adjClose']).diff().as_matrix()[1:]

# estimate parameters
sigma = np.std(r)
mu = np.mean(r) +0.5*sigma*sigma

# simulate paths
T = 20 # number of periods to simulate
N = 100 # number of scenarios
epsilon = normal(size=[T, N])
paths = data['adjClose'][-1]*np.exp(np.cumsum(mu-0.5*sigma*sigma +sigma*epsilon, axis=0))

# output
print 'data from %s to %s' % (data.index[0].date(), data.index[-1].date())
print '%d scenarios of %d periods' % (N, T)
print paths[-1]