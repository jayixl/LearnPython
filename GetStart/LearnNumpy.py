# -*- coding: utf-8 -*-
"""
Created on Wed Jun 29 22:23:43 2016

@author: jayjin
"""

import numpy as np

a = np.array([2,3,4])
print type(a)

b = np.arange(15).reshape(3,5)

c = np.array([a, a * 2])

print c
print type(c)

d = np.random.standard_normal([10, 10])

print d