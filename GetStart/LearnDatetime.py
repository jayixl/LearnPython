# -*- coding: utf-8 -*-
"""
Created on Fri Jul 15 23:20:21 2016

@author: jayjin
"""

import time
import sys
from datetime import date
from datetime import timedelta
import DateTimeUtils

#sys.path.insert(1, "../Stock")

# https://docs.python.org/2/library/datetime.html#datetime-objects
#datetime1 = datetime
#datetime2 = datetime1 + timedelta

#print date.today()
#print timedelta(days = 2, hours = 50) 
#print date.today() + timedelta(days = 2, hours = 50)
#print time.time()

print DateTimeUtils.is_valid_trading_day('a')
print DateTimeUtils.is_valid_trading_day('2016-03-27')
print DateTimeUtils.is_valid_trading_day('c')
print DateTimeUtils.is_valid_trading_day('d')
print DateTimeUtils.is_valid_trading_day('2016-03-01')
print DateTimeUtils.is_valid_trading_day('f')

print '============== amazing splitor =============='

prev = DateTimeUtils.get_prev_training_days('1995-02-05', 100)
print prev
print len(prev)

prev2 = DateTimeUtils.get_prev_training_days('2015-02-05', 100)
print prev2
print len(prev2)


#print DateTimeUtils._trading_days