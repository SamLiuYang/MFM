# -*- coding: utf-8 -*-
"""
Created on Mon Mar  5 14:59:20 2018
输入指定日做空标的，返回对应做多标的
@author: liuyang
"""
from datetime import datetime
import numpy as np
import pandas as pd
import MYSQLPD as sql
import warnings
warnings.filterwarnings("ignore") 
#定义股票类
class Stock(object):
    def __init__(self, col,):
        if isinstance(col,int):
            self.ID =0
            self.NAME=np.NaN
            self.MV=np.NaN
            self.SW1=np.NaN
            self.SW2=np.NaN
            self.DR=np.NaN
        else:
            self.ID = col['STOCKID']
            self.NAME=col['STOCKNAME']
            self.MV=col['MARKETVALUE']
            self.SW1=col['SWLV1']
            self.SW2=col['SWLV2']
            self.DR=col['NDRET']
    def listself(self):
        selflist=[self.ID,self.NAME]
        return selflist

def DateToDigit(date):
    digitdate=date.year*10000+date.month*100+date.day
    return digitdate

#计算组合净值
def LSReturn(DH):
    DHS=DH.iloc[0]
    if int(DHS.Short1)==0 and int(DHS.Short2)==0:
            PR=0
    elif int(DHS.Short1)==0:
            PR=0.25*(float(DHS.L2NR)-float(DHS.S2NR))
    elif int(DHS.Short2)==0:
            PR=0.25*(float(DHS.L1NR)-float(DHS.S1NR))
    else:
            PR=0.25*(float(DHS.L1NR)-float(DHS.S1NR)+float(DHS.L2NR)-float(DHS.S2NR))
    return PR

def ShortReturn(DH):
    DHS=DH.iloc[0]
    if int(DHS.Short1)==0:
            PR=0
    elif int(DHS.Short2)==0:
            PR=0.5*(-float(DHS.S1NR))
    else:
            PR=0.5*(-float(DHS.S1NR)-float(DHS.S2NR))
    return PR        

#从excel持仓表中导出做空持仓数据
print('Loading ShortStocks...')

df0=pd.read_excel(r"D:\Sam\项目研究\事件驱动\交易清单\20180316下周做空标的.xlsx")
SSL=df0.loc[:,['初始持仓日','代码','解禁比例','解禁类型','最后持仓日']]
#输入时间日期
BuyDate=datetime(2018,3,19)
digitBD=DateToDigit(BuyDate)

#输入匹配选股日期
PickDate=datetime(2018,3,16)
digitPD=DateToDigit(PickDate)



SS=SSL[SSL['初始持仓日']==BuyDate]
SS=SS.sort_values(by=['解禁类型','解禁比例'],ascending=False)
SS=SS.reset_index(drop=True)
#print (SS)



dbname1='tyb_stock'
dbname2='tyb_wen'
SQLstrHead1='select * from stockdaily_basic where TRADEDATE='
SQLstrHead2='select * from mfm_backtest_top_stocks where date='
SQLstr1=SQLstrHead1+str(digitPD)
SQLstr2=SQLstrHead2+str(digitPD)
AS=sql.toDF(SQLstr1,dbname1)
MFS=sql.toDF(SQLstr2,dbname2)
LS=pd.DataFrame()
for i in MFS.index:
    L=int(MFS.iloc[i,1][2:])
    LCol=AS[AS['STOCKID']==L]
    LS=LS.append(LCol)

Record=pd.DataFrame()
#MFLSRecord=pd.DataFrame()       
for N in SS.index:
    S=int((SS.iloc[N,1])[0:6])
    SCol=AS[AS['STOCKID']==S]
    SCol=SCol.iloc[0]
    Short=Stock(SCol)
    SInd=SCol['SWLV1']
    SMV=SCol['MARKETVALUE']
    LC=LS[LS['SWLV1']==SInd]
    LC['MVDiff']=LC['MARKETVALUE'].map(lambda x: abs(x-SMV))
    LC=LC.sort_values(by='MVDiff',ascending=True)  #最接近流通市值匹配
    #LC=LC.sort_values(by='AMOUNT',ascending=False)     #最大成交金额
    LCol=LC.iloc[0]
    Long=Stock(LCol)        
    if len(LC)>1:
        LCol2=LC.iloc[1]
        Long2=Stock(LCol2)
    else:
        Long2=Stock(0)
    DHList=np.array(Short.listself()+Long.listself()+Long2.listself()+[digitBD])
    DHList=DHList.reshape((1,7))
    
    DayHold=pd.DataFrame(DHList,index=[N],columns=['空代码','空名称','多代码','多名称','备选多代码','备选多名称','日期'])
    Record=Record.append(DayHold)
    #if N==100:
        #break

#

FileName=(str(digitBD)+'多空清单.csv')
#Record.to_csv(FileName,encoding='utf_8_sig')