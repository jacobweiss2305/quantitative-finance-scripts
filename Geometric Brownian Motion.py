# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 15:04:51 2018

@author: jweiss
"""

import matplotlib.pyplot as plt
import numpy as np

#Random Matrix
T = 100
dt = 0.01
N = round(T/dt)
t = np.linspace(0, T, N)
W = np.random.standard_normal(size = N) 
W = np.cumsum(W)*np.sqrt(dt) 

### standard brownian motion ###
mu = 0.01
sigma = 0.3
S0 = 100
X = (mu-0.5*sigma**2)*t + sigma*W 
S = S0*np.exp(X) ### geometric brownian motion ###
plt.plot(t, S)
plt.show()






def gen_paths(S0, r, sigma, T, M, I):
    dt = float(T) / M
    paths = np.zeros((M + 1, I), np.float64)
    paths[0] = S0
    for t in range(1, M + 1):
        rand = np.random.standard_normal(I)
        paths[t] = paths[t - 1] * np.exp((r - 0.5 * sigma ** 2) * dt +
                                         sigma * np.sqrt(dt) * rand)
    return paths

S0 = 100
K = 100
r = 0.05
sigma = 0.50
T = 1
N = 252
deltat = T / N
i = 1000
discount_factor = np.exp(-r * T)

np.random.seed(123)
paths = gen_paths(S0, r, sigma, T, N, i)
plt.show()


S0 = 100 #initial stock price
K = 100 #strike price
r = 0.05 #risk-free interest rate
sigma = 0.50 #volatility in market
T = 1 #time in years
N = 100 #number of steps within each simulation
deltat = T/N #time step
i = 1000 #number of simulations
discount_factor = np.exp(-r*T) #discount factor

S = np.zeros([i,N])
t = range(0,N,1)



for y in range(0,i-1):
    S[y,0]=S0
    for x in range(0,N-1):
        S[y,x+1] = S[y,x]*(np.exp((r-(sigma**2)/2)*deltat + sigma*deltat*np.random.normal(0,1)))
    plt.plot(t,S[y])

plt.title('Simulations %d Steps %d Sigma %.2f r %.2f S0 %.2f' % (i, N, sigma, r, S0))
plt.xlabel('Steps')
plt.ylabel('Stock Price')
plt.show()














