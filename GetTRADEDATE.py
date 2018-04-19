# -*- coding: utf-8 -*-
"""
Created on Fri Mar 30 15:45:30 2018

@author: liuyang
"""

from datetime import datetime
import numpy as np
import pandas as pd
#import PerformEval as fpe
import MYSQLPD as sql

SQLstr1='select distinct TRADEDATE from stockdaily_factor1 where TRADEDATE<=20000101'

df0 = sql.toDF(SQLstr1,'tyb_stock')
df1=pd.read_csv('tradedate00-18.csv',encoding='utf_8_sig',parse_dates=[0])
df1['TRADEDATE']=df1['TRADEDATE'].apply(lambda x: x.date())
df2=df0.append(df1)
df2=df2.reset_index(drop=True)
df2=df2.sort_values(by=['TRADEDATE'],ascending=True)
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