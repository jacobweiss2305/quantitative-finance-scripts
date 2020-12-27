# -*- coding: utf-8 -*-
"""
Created on Mon Jun 18 07:51:57 2018

@author: jweiss
"""

import numpy as np
import matplotlib.pyplot as plt
import  numpy.random  as nr
import scipy.linalg as LA

#examples of martingales
n = 1000
tSeq = np.arange (1/float(n), 1, 1/float(n))
n = len (tSeq)
sig = np.zeros ((n,n), dtype='float64')
for i in range (n):
    sig[i,0:i] = tSeq[0:i]
    sig[i,i:] = tSeq[i]
a = sig
b = tSeq
c = np.transpose (b)
muCond = np.zeros (n)
sigCond = a - np.outer(c, b)
sigCondSqrt = LA.cholesky (sigCond, lower=True)
z = muCond + np.dot (sigCondSqrt, nr.randn(n))
z = np.insert (z, 0, 0)
plot(z)

#source code
#---https://www4.stat.ncsu.edu/~laber/brownian.html--#