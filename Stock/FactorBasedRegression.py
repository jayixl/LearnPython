# -*- coding: utf-8 -*-
"""
Created on Mon Jul 18 00:03:22 2016

@author: jayjin
"""

import sys

import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet

import DateTimeUtils

sys.path.insert(1, '..\\MySql')
import MySqlUtils

_factors = ['tclose', 'chg', 'ma10']
_used_factors = ['chg', 'ma10']
_observe_shift = 20
_observe_range = 100


def get_single_stock_reg_data(df, factors, training_days, shift=20):
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

    is_test_data = 1 == len(training_days)

    # here we are using mixed integer and label based access, only .ix supports
    df = df.ix[training_days, factors]

    if False == is_test_data:
        df['fwdPrice'] = df['tclose'].shift(-shift)
        df['return'] = df['fwdPrice'] / df['tclose'] - 1
        df = df[:-shift]

    # calculate the ratio of rows that contain null
    criteria_any_null = df.isnull().any(1)
    ratio = df[criteria_any_null].shape[0] * 1.0 / criteria_any_null.count()
    if ratio > 0.2:
        return None

    # as we need to do interpolate() for missing values,
    # we should guarantee that the first and last row contain no null
    # give each one a chance '''
    if False == is_test_data:
        if df.ix[0].isnull().any():
            df = df.ix[1:]
        if df.ix[-1].isnull().any():
            df = df.ix[0:-1]

    if df.ix[0].isnull().any() or df.ix[-1].isnull().any():
        return None

        # remove invalid rows (i.e., the rows containing null)
    # all(1) indicates apply all() on rows
    # '&' is element-wise and
    criteria_no_null = df.notnull().all(1) & df['tclose'] != 0
    # & df['topen'] != 0 & df['high'] != 0 & df['low'] != 0
    df = df[criteria_no_null]  # criteria is a boolean Series

    return df


def get_stock_reg_data(universe, training_days):
    '''
    training_days must be confirmed to be existing days, and any one could be filtered if it is invalid
    '''

    data_all = {}
    start_day = training_days[0]
    end_day = training_days[-1]

    for symbol in universe:
        query_res = MySqlUtils.mysql_histrade_query(symbol, start_day, end_day)
        data = [[col for col in row] for row in query_res]
        df = pd.DataFrame(data, index=[t[1] for t in data], columns=MySqlUtils.db_columns)

        # we can't get column name info by lambda
        # df = df.apply(lambda col: pd.to_numeric(col, errors='coerce'))
        # df = df.apply(lambda col: col.astype('float'))
        for col_name in df.columns:
            if (col_name != 'symbol' and col_name != 'date' and col_name != 'name'):
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                df[col_name] = df[col_name].interpolate()

        df2 = get_single_stock_reg_data(df, _factors, training_days, _observe_shift)
        if df2 is not None:
            data_all[symbol] = df2

    return data_all


def get_regression_result(df):
    # df = df[_used_factors]
    # print df
    for f in _used_factors:
        if df[f].std() == 0:
            continue
        df[f] = (df[f] - df[f].mean()) / df[f].std()

    x = []
    for f in _used_factors:
        x.append(df[f].tolist())
    x = np.column_stack(tuple(x))
    x = np.array([np.append(row, 1) for row in x])

    y = df['return'].tolist()

    en = ElasticNet(fit_intercept=True, alpha=0)
    en.fit(x, y)
    res = en.coef_[:-1]
    w = dict(zip(_used_factors, res))
    return w


def training(universe, day):
    '''
    '''

    prev_training_days = DateTimeUtils.get_prev_training_days(day, _observe_range)
    print 'invalid training days\' count: %d' % len(prev_training_days)
    data_all = get_stock_reg_data(universe, prev_training_days)

    means, vols, weights = {}, {}, {}
    for symbol, df in data_all.iteritems():
        means[symbol] = dict(df.mean())
        vols[symbol] = dict(df.std())
        weights[symbol] = get_regression_result(df)

    return means, vols, weights


def predict():
    '''
    '''
    universe = ['000001', '000002']
    day = '2014-12-01'

    if False == DateTimeUtils.is_valid_trading_day(day):
        return None

    means, vols, weights = training(universe, day)
    data_test = get_stock_reg_data(universe, [day])

    score = {}
    for symbol in universe:
        if symbol not in weights:
            continue

        df_test = data_test[symbol]
        sum = 0
        for f in _used_factors:
            x = df_test[f][-1]  # actually should contain only 1 row
            x = (x - means[symbol][f]) / vols[symbol][f]
            sum += weights[symbol][f] * int(round(x))

        score[symbol] = sum

    print score


predict()
