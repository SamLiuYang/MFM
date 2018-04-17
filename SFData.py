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

def DatetoDigit(date):
    digitdate=date.year*10000+date.month*100+date.day
    return digitdate


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
        
def GetDayReturn(date):    
        digitdate=DatetoDigit(date)
        dbname1='tyb_stock'
        #SQLstrHead1='select * from stockdaily_basic where TRADEDATE='
        SQLstrHead1='select TRADEDATE,STOCKID,BFQCLOSE,HFQCLOSE,DAYRETURN,TRADABLE from stockdaily_basic where TRADEDATE='
        SQLstr1=SQLstrHead1+str(digitdate)
        #SQLstr2=SQLstrHead2+str(digitPD)
        DF=sql.toDF(SQLstr1,dbname1)
        DF=DF.rename(columns={'DAYRETURN':'RETURN'})
        #DF=DF[['TRADEDATE','STOCKID','BFQCLOSE','DAYRETURN','TRADABLE']]
        #DF=DF.loc[(DF['SWLV1']!=0)&(DF['TRADABLE']==1)]
        return DF

def GetIntervalReturn(d1,d2):
 
    DF1=GetReturnData(d1)
    DF2=GetReturnData(d2)
    DFL=DF1[['STOCKID','HFQCLOSE']]
    DFL=DFL.rename(columns={'HFQCLOSE':'LASTCLOSE'})
    DFM=pd.merge(DF2, DFL, how='left',on='STOCKID')
    DFM['RETURN']=(DFM['HFQCLOSE']-DFM['LASTCLOSE'])*100/DFM['HFQCLOSE']
    DFR=DFM[['TRADEDATE','STOCKID','BFQCLOSE','TRADABLE','RETURN']]
    return DFR

def GetFactor(date):    
        digitdate=DatetoDigit(date)
        dbname1='tyb_stock'
        #SQLstrHead1='select * from stockdaily_basic where TRADEDATE='
        SQLstrHead1='select TRADEDATE,STOCKID,MARKETVALUE,SWLV1,SWLV2,TRADABLE,BP from stockdaily_factor1 where TRADEDATE='
        SQLstr1=SQLstrHead1+str(digitdate)
        #SQLstr2=SQLstrHead2+str(digitPD)
        DF=sql.toDF(SQLstr1,dbname1)
        print(date)
        #DF=DF.rename(columns={'DAYRETURN':'RETURN'})
        #DF=DF[['TRADEDATE','STOCKID','BFQCLOSE','DAYRETURN','TRADABLE']]
        #DF=DF.loc[(DF['SWLV1']!=0)&(DF['TRADABLE']==1)]
        return DF


starttime = datetime.now() 
#设定回测起止日期
BegT=datetime(2007,1,1)
EndT=datetime(2017,12,31)

TDList=GetTradeDates(BegT,EndT,Freq='D')
TDNum=len(TDList)
AllData=pd.DataFrame()

Data=[GetFactor(TDList[i]) for i in range(0,TDNum)]
    

AllData=pd.concat(Data,ignore_index=True)
   


"""
for i in range(0,TDNum):
    d0=TDList[i]
    ASDF=GetDayReturn(d0)
    AllData=pd.concat([AllData,ASDF])
    print (d0)
"""
"""
for i in range(1,TDNum):
    d0=TDList[i-1]
    d1=TDList[i]
    ASDF=GetIntervalReturn(d0,d1)
    AllData=pd.concat([AllData,ASDF])
    print (d1)
"""
stoptime = datetime.now() 
print(stoptime-starttime)