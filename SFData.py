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
        TDList=TDDF[TDDF['IsWeekEnd']==1]
        return TDList
        



#设定回测起止日期
BegT=datetime(2007,1,1)
EndT=datetime(2017,12,31)

TD=GetTradeDates(BegT,EndT,Freq='W')
