# -*- coding: utf-8 -*-
"""
Created on Mon Jul 18 00:03:22 2016

@author: jayjin
"""

import os
import sys
import numpy as np
import pandas as pd

import DateTimeUtils

sys.path.insert(1, '../MySql')
import MySqlUtils


_factors = ['tclose', 'ma10']


def SingleStockRegDataGet(df, factors, training_days, shift = 20):
    """ 
    get factor data from original df
        
    @parameters
    ----------
    df : DataFrameWork
        data source
    factors : string list
        selected factors
    training_days : string list
        end date, format ['yyyy-MM-dd', ...]
    shift : integer
        offset of days to calculate forward price    
    """

    # here we are using mixed integer and label based access, only .ix supports
    df = df.ix[training_days, factors] 
    
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
    criteria_no_null = df.notnull().all(1) # all(1) indicates apply all() on rows
    df = df[criteria_no_null] # criteria is a boolean Series
    
    return df

def StockRegDataGet(universe, training_days):
        
    '''
    training_days must be confirmed to be existing days, but any one could be filtered if it is invalid
    '''
    
    data_all = {}
    start_day = training_days[0]
    end_day = training_days[-1]
    
    for symbol in universe:
        query_res = MySqlUtils.mysql_histrade_query(symbol, start_day, end_day)
        data = [[col for col in row] for row in query_res]
        df = pd.DataFrame(data, index = [t[1] for t in data], columns = MySqlUtils.db_columns)
        
        # we can't get column name info by lambda
        #df = df.apply(lambda col: pd.to_numeric(col, errors='coerce'))
        #df = df.apply(lambda col: col.astype('float'))
        for col_name in df.columns:
            if (col_name != 'symbol' and col_name != 'date' and col_name != 'name'):
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce') 
                df[col_name] = df[col_name].interpolate()
                
        
        df2 = SingleStockRegDataGet(df, _factors, training_days, 5)
        print df2
    
    return data_all

prev_training_days = DateTimeUtils.get_prev_training_days('2015-02-05', 100)
print len(prev_training_days)
StockRegDataGet(['000001'], prev_training_days)   