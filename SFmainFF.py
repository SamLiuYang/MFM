# -*- coding: utf-8 -*-
"""
Created on Fri Mar 23 09:02:02 2018

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

def GetASDF(date):
    AS=DayReturnDF[DayReturnDF['TRADEDATE']==date]
    ASni=AS.set_index('STOCKID')
    ASni=ASni[['DAYRETURN']]
    return ASni

def FactorStandardize(DF):
    return DF

class Portfolio(object):
    def __init__(self,stocklist,tradedate=0,weight='EqualAmount'):
        self.TRADEDATE=tradedate
        self.Stocklist=stocklist
        Port=pd.DataFrame(stocklist,columns=['STOCKID'])
        if weight=='EqualAmount':
            ew=1/len(Port)
            Port['Weight']=ew
        self.holding=Port
        
    def GetReturn(self,ASDF=0,fetch=0):
        PT=self.holding.copy()
        PT=PT.set_index('STOCKID')
        PT2=pd.concat([PT,ASDF],join='inner',axis=1)
        """
        for id in PT.index:
            ret=ASDF.at[id,'DAYRETURN']
        
            if len(retDF)==0:
                print(id)
                raise Exception('Cannot Find Return for ID')
            ret=retDF.iat[0]
       
            PT.at[id,'DAYRET']=ret
        """
        PortDayRet=sum(PT2.Weight*PT2.DAYRETURN)
        return PortDayRet

        

class GroupedPort(object):
    def __init__(self,GroupNum,PFList,tradedate=0):
        self.TRADEDATE=tradedate
        self.GN=GroupNum
        if GroupNum!=len(PFList):
            raise Exception('Input Group Number Error') 
        else:
            GPList=[]
            for G in range(1,GroupNum+1):
                GPList.append(PFList[G-1])
            self.GroupPort=GPList
    
    def GetReturns(self,ASDF=0,fetch=0):
        GRDF=pd.DataFrame(columns=['DAYRET'])
        for GI in range(0,self.GN):
            port=self.GroupPort[GI]
            ret=port.GetReturn(ASDF)
            #GRDF.loc[GI,'GroupIndex']=GI+1
            GRDF.loc[GI+1,'DAYRET']=ret
            #print ([GI+1,ret])
        return GRDF
            
#回测主程序        
class SFBacktest(object):    
    def __init__(self,BegT,EndT,SR='ALL',GN=10,TF='W'):
        self.BegT=BegT
        self.EndT=EndT
        self.StockRange=SR
        self.GroupNum=GN
        self.TradeFreq=TF
        #读取交易日期列表
        TDs=pd.read_csv(r"D:\Sam\PYTHON\Tradedates.csv",encoding='utf_8_sig',parse_dates=[0])
        BTTD=TDs[(TDs['TRADEDATE']>=BegT)&(TDs['TRADEDATE']<=EndT)]
        self.BTTD=BTTD
        if TF=='W':
            TDDF=BTTD[['TRADEDATE','IsWeekEnd']]
            TDDF=TDDF.rename(columns={'IsWeekEnd':'ChangePos'})
        self.TDDF=TDDF
    
    def GetFactors(self,date):
        digitdate=date.year*10000+date.month*100+date.day
        dbname1='tyb_stock'
        #dbname2='tyb_wen'
        SQLstrHead1='select * from stockdaily_factor1 where TRADEDATE='
        #SQLstrHead2='select * from mfm_backtest_top_stocks where date='
        SQLstr1=SQLstrHead1+str(digitdate)
        #SQLstr2=SQLstrHead2+str(digitPD)
        DF=sql.toDF(SQLstr1,dbname1)
        DF=DF[['STOCKID','TRADABLE','SWLV1','BP']].copy()
        DF=DF.loc[(DF['SWLV1']!=0)&(DF['TRADABLE']==1)]
        return DF
    
    def SortNGroup(self,DFS,FactorName):
        DFS=(DFS.sort_values(by=FactorName,ascending=False)).reset_index(drop=True)
        length=len(DFS)
        rank=pd.Series(range(0,length))
        group=rank*(self.GroupNum)/length
        DFS['GROUP']=group
        DFS['GROUP']=DFS['GROUP'].apply(lambda x:int(x)+1)
        GSL=[]
        for i in range(1,self.GroupNum+1):
           GSL.append([]) 
           SGDF=DFS.loc[DFS['GROUP']==i]
           SL=list(SGDF['STOCKID'])
           GSL[i-1]=Portfolio(SL)
        GP=GroupedPort(self.GroupNum,GSL)
        return GP    
    
    def SFtoGroup(self,date):
        DF=self.GetFactors(date)
        DFS=FactorStandardize(DF)
        SortedDFS=self.SortNGroup(DFS,'BP')
        #print (SortedDFS)
        return SortedDFS
        
        
    def GetBTDates(self):    #整理交易日DF，从第一个换仓日开始回测
        TDDF=self.TDDF.reset_index(drop=True)
        length=len(TDDF)
        for i in TDDF.index:
            if TDDF.iloc[i,1]==1:
                break
        TDDF=TDDF.loc[i:length].reset_index(drop=True)
        return TDDF
    
    def backtest(self):
       
        TDDF=self.GetBTDates()
        DL=len(TDDF)
        #初始交易日，仅获得分组仓位
        idate=TDDF.iloc[0,0]
        print (idate)
        GP=self.SFtoGroup(idate)
        #按顺序遍历每个交易日，计算分组收益，逢交易日则进行换仓操作
        RetTable=pd.DataFrame(columns=range(1,self.GroupNum+1)) 
        #print(RetTable.columns)
        for N in range(1,DL):
            date=TDDF.iloc[N,0]
            ASDF=GetASDF(date)
            #print(ASDF)
            Ret=GP.GetReturns(ASDF)
            RL=list(Ret.DAYRET)
            RetTable.loc[date]=RL
            print(date)
            if TDDF.iloc[N,1]==1:
                 GP=self.SFtoGroup(date)
                 
            #if N==2:
                #break
        #return ASDF
                        
        #print(RetTable)
        return RetTable



starttime = datetime.now() 
print(starttime)
"""读取本地日读数据作为回测"""

f = open('D:\Sam\PYTHON\DayReturn2007-2017.csv',encoding='UTF-8')
reader = pd.read_csv(f, sep=',', iterator=True,parse_dates=[1])
loop = True
chunkSize = 100000
chunks = []
while loop:
    try:
        chunk = reader.get_chunk(chunkSize)
        chunks.append(chunk)
    except StopIteration:
        loop = False
        #print("Iteration is stopped.")
DRDF = pd.concat(chunks, ignore_index=True)
DRDF.drop('OldIndex', axis=1, inplace=True)
DRDF['TRADEDATE']=DRDF['TRADEDATE'].apply(DatetoDigit)
#DRDF=DRDF.set_index(['TRADEDATE','STOCKID'])
print('Done')



               
#参数设置

#设定回测起止日期
BegT=datetime(2017,1,1)
EndT=datetime(2017,12,31)


#设定股票选取范围(ALL,300,500,800)
StockRange='ALL'

#设定分组数量
GroupNum=10

#设定调仓频率(D,W,M)
TradeFreq='W'

#创建主程序
#BT=SFBacktest(BegT,EndT,GN=GroupNum)

#A=BT.backtest()
"""
SL1=[1,2,4,5]
SL2=[6,7,8,9,10]
P1=Portfolio(SL1)
P2=Portfolio(SL2)
G1=GroupedPort(2,[P1,P2])
#ASDF=BT.GetFactors(datetime(2017,1,5))
#print(ASDF)
#AA=BT.SortNGroup(ASDF,'BP')
ASDF=GetASDF(datetime(2017,1,6))
#DayFactor=BT.GetFactors(datetime(2017,1,5))
"""
stoptime = datetime.now() 
print(stoptime)
print(stoptime-starttime)