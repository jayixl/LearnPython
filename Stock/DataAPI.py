# -*- coding: utf-8 -*-
"""
Created on Tue Jul 05 21:55:55 2016

@author: jayjin
"""

import pandas as pd
import numpy  as np

def StockFactorsGet(symbols, trading_days):
    
    """ query history trades with symbol, start date, end date
        
    Parameters
    ----------
    symbols : list string
        stock symbol codes.
    trading_days : list string
        trading days
    """ 
    
    