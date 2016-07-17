# -*- coding: utf-8 -*-
"""
Created on Sun Jul 17 08:28:32 2016

@author: jayjin
"""

import sys
import time
import numpy as np
import MySqlUtils
from datetime import date
from datetime import timedelta

#sys.path.insert(1, '../MySql')

# https://docs.python.org/2/library/datetime.html#datetime-objects
#datetime1 = datetime
#datetime2 = datetime1 + timedelta

_trading_days_set = set()
_trading_days_sorted = np.array([])

def _get_trading_days():
    query = 'SELECT DISTINCT date FROM netease_stock.histrade_day ORDER BY date'
    all_trading_days = MySqlUtils.mysql_generic_query(query)
    
    global _trading_days_sorted
    _trading_days_sorted = np.array(sorted([row[0] for row in all_trading_days]))
    _trading_days_set.update(_trading_days_sorted)
    print _trading_days_sorted

def is_valid_trading_day(day):
    if 0 == len(_trading_days_set):
        _get_trading_days()
    
    return day in _trading_days_set
    
    
def get_prev_training_days(current_day, prev_num):
    if 0 == len(_trading_days_set):
        _get_trading_days()
        
    return _trading_days_sorted[_trading_days_sorted < current_day][-prev_num:]