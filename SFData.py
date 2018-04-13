# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 15:42:52 2018

@author: liuyang
"""

from datetime import datetime
import numpy as np
import pandas as pd
import MYSQLPD as sql
import math


#读取交易日期列表
def GetTradeDates(BegT,EndT,Freq='D'):
    TDs=pd.read_csv(r"D:\Sam\PYTHON\Tradedates.csv",encoding='utf_8_sig',parse_dates=[0])
    BTTD=TDs[(TDs['TRADEDATE']>=BegT)&(TDs['TRADEDATE']<=EndT)]
    if Freq=='W':
        TDDF=BTTD[['TRADEDATE','IsWeekEnd']]
        TDList=TDDF[TDDF['IsWeekEnd']==1].TRADEDATE.tolist()
        return TDList
    elif Freq=='D':
        TDList=BTTD[['TRADEDATE']].TRADEDATE.tolist()
        return TDList
        
def GetReturnData(date):    
        digitdate=date.year*10000+date.month*100+date.day
        dbname1='tyb_stock'
        #SQLstrHead1='select * from stockdaily_basic where TRADEDATE='
        SQLstrHead1='select TRADEDATE,STOCKID,BFQCLOSE,DAYRETURN,TRADABLE from stockdaily_basic where TRADEDATE='
        SQLstr1=SQLstrHead1+str(digitdate)
        #SQLstr2=SQLstrHead2+str(digitPD)
        DF=sql.toDF(SQLstr1,dbname1)
        #DF=DF[['TRADEDATE','STOCKID','BFQCLOSE','DAYRETURN','TRADABLE']]
        #DF=DF.loc[(DF['SWLV1']!=0)&(DF['TRADABLE']==1)]
        return DF




starttime = datetime.now() 
#设定回测起止日期
BegT=datetime(2007,1,1)
EndT=datetime(2017,12,31)

TDList=GetTradeDates(BegT,EndT,Freq='D')

AllData=pd.DataFrame()
for date in TDList:
    ASDF=GetReturnData(date)
    AllData=pd.concat([AllData,ASDF])
    print (date)

stoptime = datetime.now() 
print(stoptime-starttime)