# -*- coding: utf-8 -*-
"""
Created on Sun Nov 08 13:23:32 2015

@author: jayjin
"""
import MySQLdb
import os
import pandas as pd

_host = 'localhost'
_user = 'root'
_password = '123456'
db = 'trade'
histrade_columns = ['symbol', 'date', 'name', 'topen', 'tclose', 'high', 'low', 'lclose', 'chg', 'pchg', 'turnover',
                    'voturnover', 'vaturnover', 'tcap', 'mcap', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60']

fundflow_columns = ['symbol', 'date', 'tclose', 'pchg', 'turnover', 'inflow', 'outflow', 'netinflow', 'maininflow',
                    'mainoutflow', 'mainnetinflow']


def mysql_test():
    try:
        conn = MySQLdb.connect(host=_host, user=_user, passwd=_password, db=db, port=3306, charset='utf8')
        cur = conn.cursor()
        cur.execute('select * from codelist')
        res = cur.fetchall()
        data = [[col for col in row] for row in res]
        print data
        df = pd.DataFrame(data)
        df.to_csv("e:/test/out.csv", encoding='utf-8')
        print df


        # f = open('e:/test/out.txt', 'w')
        # f.write("".join(top1.))
        # f.close()

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    finally:
        cur.close()
        conn.close()


def mysql_generic_query(query):
    '''
    generic query
    '''
    try:
        conn = MySQLdb.connect(host=_host, user=_user, passwd=_password, db=db, port=3306, charset='utf8')
        cur = conn.cursor()
        cur.execute(query)
        res = cur.fetchall()
        return res

    except MySQLdb.Error, e:
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
        WHERE symbol = '%s' AND date >= '%s' AND date <= '%s' ''' % (','.join(histrade_columns), symbol, start, end)

    try:
        conn = MySQLdb.connect(host=_host, user=_user, passwd=_password, db=db, port=3306, charset='utf8')
        cur = conn.cursor()
        cur.execute(query)
        # print 'there are %s rows record' % count
        res = cur.fetchall()
        return res

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    finally:
        cur.close()
        conn.close()

    return None


def fund_flow_query(symbol, start, end):
    """
    :param symbol:
    :param start:
    :param end:
    :return:
    """

    query = '''SELECT %s FROM fund_flows
        WHERE symbol = '%s' AND date >= '%s' AND date <= '%s' ''' % (','.join(fundflow_columns), symbol, start, end)

    try:
        conn = MySQLdb.connect(host=_host, user=_user, passwd=_password, db=db, port=3306, charset='utf8')
        cur = conn.cursor()
        cur.execute(query)
        # print 'there are %s rows record' % count
        res = cur.fetchall()
        return res

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    finally:
        cur.close()
        conn.close()

    return None


mysql_test()
