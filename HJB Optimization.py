# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 06:55:13 2018

@author: jweiss
"""

import numpy as np
from   scipy.sparse import spdiags, identity
from   scipy.sparse.linalg import spsolve
import matplotlib.pyplot as plt
import pandas as pd
%matplotlib inline  

#tickers_list = ['AAPL','FDS']
#start = '01/01/2016'
#end = '08/01/2018'   
def priceScraper(tickerList, start, end):
    columns = ['timetrade','open','High','Low','Close','Volume','adjClose']
    url_list = [str('https://finance.yahoo.com/quote/'+i+'/history?period1='
                + str(int(time.mktime(datetime.datetime.strptime(start, "%m/%d/%Y").timetuple())))
                +'&period2='+str(int(time.mktime(datetime.datetime.strptime(end, "%m/%d/%Y").timetuple())))
                +'&interval=1d&filter=history&frequency=1d' ) for i in tickerList]
    BeautifulSoup_List = [str(BeautifulSoup(i, 'html.parser')) for i in [urlopen(i) for i in url_list]]
    text_start = "HistoricalPriceStore"
    text_end = "isPending"
    raw_data = [i[i.find(text_start)+len(text_start):i.rfind(text_end)][13:len(i)-3] for i in BeautifulSoup_List]
    pandas_list = [pd.DataFrame([[i.split(',') for i in [re.sub('[^0-9\.\}\,]+','',i).split('},') 
                    for i in raw_data][j] if len(i) > 1] for j in range(len([re.sub('[^0-9\.\}\,]+','',i).split('},') 
                    for i in raw_data]))][k]) for k in range(len(raw_data))]
    for i in range(len(pandas_list)):
        pandas_list[i].columns = columns
        pandas_list[i] = pandas_list[i].dropna()
        pandas_list[i] = pandas_list[i][['adjClose']]
        pandas_list[i] = pandas_list[i].apply(pd.to_numeric)
    return pandas_list


CENTRAL =0
FORWARD =1
BACKWARD= 2

def main():
    r     = 0.03
    sigma = 0.30
    xi    = 0.33
    pi    = 0.1
    W0    = 1.0
    T     = 20.0
    gamma = 14.47
    Wmin  = 0.0
    Wmax  = 5.0
    M     = 1600
    N     = 100
    tol   = 1e-6
    scale = 1.0
    Pmax  = 1.5
    J     = 8
    hsigsq  = 0.5 * sigma ** 2
    sigmaxi = sigma * xi
    dW      = ( Wmax - Wmin) / N
    dt      = T / M
    dWsq    = dW ** 2
    
    W    = np.linspace( Wmin, Wmax, N + 1 )
    Ps   = np.linspace( 0.0, Pmax, J ) # discretize controls
    I    = identity( N + 1 )
    Gn   = np.zeros_like( W )
    Gnp1 = np.zeros_like( W )

    terminal_values = ( W - 0.5 * gamma ) ** 2
    
    def bc( t ): 
        tau = T - t
        c   = ( 2 * pi ) / r

        e1 = np.exp( r * tau )
        e2 = np.exp( 2 * r * tau )
        alpha = e2 * ( Wmax**2 )
        beta  = ( c * e2 - ( gamma + c ) *e1 ) * Wmax
        delta = ( ( gamma**2 ) / 4.0 ) +  ( ( pi * c ) / ( 2 * r ) ) * ( e2 - 1 )\
                - ( ( pi * ( gamma + c ) ) / r ) * ( e1 - 1 )  
        return alpha + beta + delta
    
    def alpha( W, p, dirn = CENTRAL ):
        t1 = hsigsq * (p**2) * (W**2) / dWsq
        t2 = ( pi + W * ( r + p * sigmaxi ) ) 
        if dirn == CENTRAL:
            return t1 - t2 / ( 2 * dW )
        elif dirn == BACKWARD:
            return t1 - t2 / dW
        elif dirn == FORWARD:
            return t1
    
    def beta( W, p, dirn = CENTRAL ):
        t1 = hsigsq * (p**2) * (W**2) / dWsq
        t2 = ( pi + W * ( r + p * sigmaxi ) ) 
        if dirn == CENTRAL:
            return t1 + t2 / (2 *dW)
        elif dirn == FORWARD:
            return t1 + t2 / dW
        elif dirn == BACKWARD:
            return t1
    
    def makeDiagMat( alphas, betas ):
        d0, dl, d2 = -( alphas + betas ), np.roll( alphas, -1 ), np.roll( betas, 1 )
        d0[-1] = 0.
        dl [-2:] = 0.
        data = np.array( [ d0, dl, d2 ] )
        diags = np.array( [ 0, -1, 1 ] )
        return spdiags( data, diags, N + 1, N + 1 )
    
    def find_optimal_ctrls( Vhat, t ):
        Fmin = np.tile( np.inf, Vhat.size )
        optdiffs = np.zeros_like( Vhat, dtype = np.int )
        optP    = np.zeros_like( Vhat )
        alphas  = np.zeros_like( Vhat )
        betas   = np.zeros_like( Vhat )
        curDiffs = np.zeros_like( Vhat, dtype = np.int )
        
        for p in Ps: # Hnd the optimal control
            alphas[:] = -np.inf
            betas[:] = -np.inf
            curDiffs[:] = CENTRAL
            
            for diff in [CENTRAL, FORWARD, BACKWARD]:
                a = alpha( W, p, diff)
                b = beta( W, p, diff )
                positive_coeff_indices = np.logical_and( a >= 0.0, b >= 0.0 ) == True
                positive_coeff_indices = np.logical_and( positive_coeff_indices, alphas==-np.inf )
                indices = np.where( positive_coeff_indices )
                alphas[ indices ] = a[ indices ]
                betas[ indices ] = b[ indices ]
                curDiffs[ indices ] = diff
            M = makeDiagMat( alphas, betas )
            F = M.dot( Vhat )
            indices = np.where( F < Fmin )
            Fmin[indices] = F[indices ]
            optP[indices] = p
            optdiffs[indices] = curDiffs[ indices ]
        return optP, optdiffs
    
    timesteps = np.linspace( 0.0, T, M + 1 )[:-1]
    timesteps = np.flipud( timesteps )
    V = terminal_values
    alphas = np.zeros_like( V )
    betas = np.zeros_like( V )

    for t in timesteps:
        Vhat = V.copy()
        Gnp1[-1] =bc(t+ dt)
        Gn[-1] = bc( t)
        B = Gn - Gnp1
        while True:
            ctrls, diffs = find_optimal_ctrls( Vhat, t )
            for diff in [ CENTRAL, FORWARD, BACKWARD ]:
                indices = np.where( diffs == diff )
                alphas[indices] = alpha( W[indices], ctrls[indices], diff )
                betas[indices] = beta( W[indices], ctrls[indices], diff )
        
            A   = makeDiagMat( alphas, betas )
            M = I - dt * A
            Vnew = spsolve( M, V + B )
            scale    = np.maximum( np.abs( Vnew ), np.ones_like( Vnew ) )
            residuals = np.abs( Vnew - Vhat ) / scale
            if np.all( residuals[:-1] < tol ):
                V = Vnew
                break
            else:
                Vhat = Vnew
    return W, V, ctrls
W, V, ctrls = main()

f, (ax1, ax2) = plt.subplots(1, 2, figsize=(16,5))
ax1.plot(W, V, color = 'blue', lw=2)
ax1.set_title('Plot of $V(W=w_0, 0)$ against wealth')
ax2.plot(W, ctrls, color = 'red', lw=2, alpha = 0.7)
_ = ax2.set_title('Plot of optimal control against wealth')



