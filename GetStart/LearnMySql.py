# -*- coding: utf-8 -*-
"""
@author: jayjin
"""

import os
import sys
import numpy as np
import pandas as pd

sys.path.insert(1, '../MySql')
sys.path.insert(1, '../Stock')

import MySqlUtils


def SingleStockRegDataGet(df, factors, end_date, start_offset, shift=20):
    """ 
    get factor data from original df
        
    @parameters
    ----------
    df : DataFrameWork
        data source
    factors : string list
        selected factors
    end_date : string
        end date, format 'yyyy-MM-dd'        
    start_offset : integer
        offset from expected start date to given end date
    shift : integer
        offset of days to calculate forward price    
    """

    # maybe we cant find end_date, locate the nearest earlier date
    # end_date = df['date'][df['date'] <= end_date][-1] # equivalent way
    end_date = df.index[df.index <= end_date][-1]

    end_loc = df.index.get_loc(end_date)
    start_loc = max(end_loc - start_offset, 0)

    # here we are using mixed integer and label based access, only .ix supports
    df = df.ix[start_loc:end_loc, factors]

    df['fwdPrice'] = df['tclose'].shift(-shift)
    df = df[:-shift]

    # calculate the ratio of rows that contain null
    criteria_any_null = df.isnull().any(1)
    ratio = df[criteria_any_null].shape[0] * 1.0 / criteria_any_null.count()
    if ratio > 0.2:
        return None

    # as we need to do interpolate() for missing values,
    # we should guarantee that the first and last row contain no null
    # give each one a chance '''
    if df.ix[0].isnull().any():
        df = df.ix[1:]
    if df.ix[-1].isnull().any():
        df = df.ix[0:-1]

    if df.ix[0].isnull().any() or df.ix[-1].isnull().any():
        return None

        # remove invalid rows (i.e., the rows containing null)
    criteria_no_null = df.notnull().all(1)  # all(1) indicates apply all() on rows
    df = df[criteria_no_null]  # criteria is a boolean Series

    return df


def StockRegDataGet(universe, training_days):
    '''
    training_days must be confirmed to be existing days, but any one could be filtered if it is invalid
    '''

    data_all = {}

    for symbol in universe:
        print symbol

    return None


query_res = MySqlUtils.mysql_histrade_query('000001', '1995-01-01', '1995-03-03')
print 'get %d rows from db' % len(query_res)

# type(query_res) is tuple tuple
data = [[col for col in row] for row in query_res]
df = pd.DataFrame(data, index=[t[1] for t in data], columns=MySqlUtils.db_columns)

# we can't get column name info by lambda
# df = df.apply(lambda col: pd.to_numeric(col, errors='coerce'))
# df = df.apply(lambda col: col.astype('float'))
for col_name in df.columns:
    if (col_name != 'symbol' and col_name != 'date' and col_name != 'name'):
        df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
        df[col_name] = df[col_name].interpolate()

factors = ['tclose', 'ma10']
df2 = SingleStockRegDataGet(df, factors, '1995-02-29', 10, 5)
print df2

# print string.join(MySqlUtils.db_columns, ',')
# print ','.join(["'%s'" % col for col in MySqlUtils.db_columns])
