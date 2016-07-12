# -*- coding: utf-8 -*-
"""
Created on Sun Jul 03 11:13:16 2016

@author: jayjin
"""

import numpy as np
import pandas as pd

df = pd.DataFrame([[10, 1], [20, 2], [30, 3], [40, 4]], columns=['numbers', 'col2'], index=['a', 'b', 'c', 'd'])

print df
print df.index

print "df.ix['a']:"
print df.ix['a']


print "df.ix[['a', 'b']]:"
print df.ix[['a', 'b']]


print "df.sum()"
print df.sum()


print df.apply(lambda x: x ** 2)
print df ** 2


df['col4'] = (5,6,7,8)
print df

dates = pd.date_range('2016-6-1', periods=9, freq='W')
print dates