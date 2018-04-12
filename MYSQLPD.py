# -*- coding: utf-8 -*-
"""
Created on Wed Mar 21 14:03:38 2018

@author: liuyang
"""
from datetime import datetime
import numpy as np
import pandas as pd
#import PerformEval as fpe
import pymysql as pms

#dbname='tyb_stock'

#sqlstr1='select * from stockdaily_basic where TRADEDATE=20100104'

def toDF(sqlstr,dbname='tyb_stock'):
    conn=pms.connect(
                host='localhost',
                port=3306,
                user='root',
                passwd='Kxbs2018!',
                charset='UTF8',
                db=dbname)
    sqldf=pd.read_sql(sqlstr,conn)
    return sqldf
"""
#cur=conn.cursor()
sqlstr='select * from stockdaily_basic where TRADEDATE=20100104'

#df = pd.read_sql(sqlstr,conn)

sqlstr2='select distinct TRADEDATE from stockdaily_basic'

df2 = pd.read_sql(sqlstr2,conn)
#df2['Weekday']=df2.Weekday()
df2['Weekday']=df2['TRADEDATE'].apply(lambda x: x.weekday())
df2['IsWeekEnd']=0
DL=len(df2)
for N in range(1,DL):
    if df2.iloc[N,1]-df2.iloc[N-1,1]<=0:
       df2.iloc[N-1,2]=1
    if (df2.iloc[N,0]-df2.iloc[N-1,0]).days>=7:
       df2.iloc[N-1,2]=1   
df2.to_csv('Tradedates.csv',encoding='utf_8_sig',index=False)       
           

df2=pd.read_csv(r"D:\Sam\PYTHON\Tradedates.csv",encoding='utf_8_sig',
                parse_dates=[0]) 
DL=len(df2)

df2['IsMonthEnd']=0
for N in range(0,DL-1):
    if (df2.iloc[N,0].month)!=(df2.iloc[N+1,0].month):
       df2.iloc[N,3]=1

df2['IsYearEnd']=0
for N in range(0,DL-1):
    if (df2.iloc[N,0].year)!=(df2.iloc[N+1,0].year):
       df2.iloc[N,4]=1

df2.to_csv('Tradedates.csv',encoding='utf_8_sig',index=False)
"""