# -*- coding: utf-8 -*-
"""
Created on Sun Nov 08 13:23:32 2015

@author: jayjin
"""
import MySQLdb
import os

host = 'localhost'
user = 'root'
password = '1234'
db = 'netease_stock'

def get_file_size(fullName):
    fileSize = os.path.getsize(fullName)
    return fileSize


def get_file_path(fullName):
    filePath = os.path.abspath(fullName)
    return filePath
    

def mysql_test():
    try:
        conn=MySQLdb.connect(host=host,user=user,passwd=password,db=db,port=3306)
        cur=conn.cursor()
        cur.execute('select * from histrade_day')
        top1 = cur.fetchone()
        print top1
        
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    finally:
        cur.close()
        conn.close()

def mysql_histrade_query(symbol, start, end):
    try:
        conn=MySQLdb.connect(host='stcvm-366',user='root',passwd='1234',db='world',port=3306)
        cur=conn.cursor()
        count = cur.execute('select * from city')
        print 'there has %s rows record' % count
        top1 = cur.fetchone()
        print top1
        cur.close()
        conn.close()
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
