# -*- coding: utf-8 -*-
"""
@author: jayjin
"""

import os
import sys
import numpy as np
import pandas as pd

sys.path.insert(0, '../MySql')

import MySqlUtils

def StockRegDataGet(df, factors, start, end, shift = 20):
    '''
    we could set end as a date, and start as a negative offset
    '''    
    
    df = df.loc[start:end, factors]
    df['fwdPrice'] = df['tclose'].shift(-shift)
    df = df[:-shift]
    
    return df


#mysql_test()
query_res = MySqlUtils.mysql_histrade_query('000001', '1998-01-03', '1998-03-03')
print len(query_res)

#data = [[(col if col is not None else 0) for col in row] for row in query_res]
data = [[col for col in row] for row in query_res]
df = pd.DataFrame(data, index = [t[1] for t in data], columns = MySqlUtils.db_columns)

#df.loc['1995-01-03', 'ma10'] = '1'

#print df['ma10'].astype('float')

#df = df.apply(lambda col: pd.to_numeric(col, errors='coerce')) #
#df = df.apply(lambda col: col.astype('float'))
#df['ma10'] = df['ma10'].interpolate()

for col_name in df.columns:
    if (col_name != 'symbol' and col_name != 'date' and col_name != 'name'):
        df[col_name] = pd.to_numeric(df[col_name], errors='coerce') 
        df[col_name] = df[col_name].interpolate()

print df['mcap'][-1]

print df

factors = ['tclose', 'ma10']
df2 = StockRegDataGet(df, factors, '1998-01-05', '1998-02-25', 20)
print df2

#print string.join(MySqlUtils.db_columns, ',')

#print ','.join(["'%s'" % col for col in MySqlUtils.db_columns])
