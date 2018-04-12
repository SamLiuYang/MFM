# -*- coding: utf-8 -*-
"""
Created on Thu Mar  1 16:11:42 2018

@author: liuyang
"""

import csv  
import sys  
import mysql.connector as sql
import pandas as pd


def getdata(SQLstr,dbname):
    conn=sql.connect(  
    host='localhost',  
    port=3306,  
    user='root',  
    passwd='Kxbs2018!',  
    #passwd='123456',  
    db=dbname)  
    cursor=conn.cursor()  

    cursor.execute(SQLstr)  

    values=cursor.fetchall() 
    conn.commit() 
    cursor.close() 
    return values
    #print (values[0]) 
    #print('OK')  
"""
SQLstr='select * from stockdaily_factor1 where TRADEDATE=20161205'
dbname='tyb_stock'
a=getdata(SQLstr,dbname)
"""