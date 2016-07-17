# -*- coding: utf-8 -*-
"""
Created on Sun Nov 08 13:23:32 2015

@author: jayjin
"""
import MySQLdb
import os

_host = 'localhost'
_user = 'root'
_password = '1234'
db = 'netease_stock'
db_columns = ['symbol', 'date', 'name', 'topen', 'tclose', 'high', 'low', 
              'lclose', 'chg', 'pchg', 'turnover', 'voturnover', 'vaturnover', 
              'tcap', 'mcap', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60']
 

def mysql_test():
        
    try:
        conn = MySQLdb.connect(host=_host,user=_user,passwd=_password,db=db,port=3306)
        cur = conn.cursor()
        cur.execute('select %s from histrade_day' % (','.join(db_columns), ))
        top1 = cur.fetchone()
        print top1
        
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        
    finally:
        cur.close()
        conn.close()

def mysql_generic_query(query):
    
    '''
    generic query
    '''
    try:
        conn = MySQLdb.connect(host=_host,user=_user,passwd=_password,db=db,port=3306)
        cur = conn.cursor()
        cur.execute(query)
        res = cur.fetchall()
        return res
        
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        
    finally:
        cur.close()
        conn.close()


def mysql_histrade_query(symbol, start, end):
    
    """ query history trades with symbol, start date, end date
        
    Parameters
    ----------
    symbol : string
        stock symbol code.
    start : string
        start date, format 'yyyy-MM-dd'
    end : string
        end date, format 'yyyy-MM-dd'
    """        
    
    query = '''SELECT %s FROM histrade_day
        WHERE symbol = '%s' AND date >= '%s' AND date <= '%s' ''' % (','.join(db_columns), symbol, start, end)
                
    try:
        conn = MySQLdb.connect(host=_host, user=_user, passwd=_password, db=db, port=3306)
        cur = conn.cursor()
        cur.execute(query)
        #print 'there are %s rows record' % count
        res = cur.fetchall()
        return res
        
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        
    finally:
        cur.close()
        conn.close()
        
    return None       
    