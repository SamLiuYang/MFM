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

"""批量加载回测数据的函数"""
def LoadData(FileName):
    f = open('D:\Sam\PYTHON\\'+FileName,encoding='UTF-8')
    reader = pd.read_csv(f, sep=',', iterator=True,parse_dates=[0])
    loop = True
    chunkSize = 100000
    chunks = []
    while loop:
        try:
            chunk = reader.get_chunk(chunkSize)
            chunks.append(chunk)
        except StopIteration:
            loop = False
    print("Iteration is stopped.")
    DRDF = pd.concat(chunks, ignore_index=True)
    #if 'OldIndex' in DRDF.index:
    #    DRDF.drop('OldIndex', axis=1, inplace=True)
    DRDF['TRADEDATE']=DRDF['TRADEDATE'].apply(DatetoDigit)
    DRDF=DRDF.set_index(['TRADEDATE','STOCKID'])
    print('Done')
    return DRDF

"""得到当期收益率数据，用于回测"""
def GetASDF(date):
    global AllDR
    dd=DatetoDigit(date)
    AS=AllDR.loc[dd]
    #ASni=AS.set_index('STOCKID')
    #ASni=AS[['DAYRETURN']]
    ASni=AS[['RETURN']]
    return ASni

"""得到当期因子数据，用于分组和选股"""
def GetFactorFF(date,MainFactor):
    global AllFactor
    dd=DatetoDigit(date)
    DF=AllFactor.loc[dd]
    DF=DF.rename(columns={MainFactor:'MF'})
    DF=DF.loc[(DF['SWLV1']!=0)&(DF['TRADABLE']==1)]
    return DF

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
        
    def GetReturn(self,DayRetDF,fetch=0):
        PT=self.holding.copy()
        PT=PT.set_index('STOCKID')
        PT2=pd.concat([PT,DayRetDF],join='inner',axis=1)
        
        self.HoldAndRet=PT2
        PortDayRet=sum(PT2.Weight*PT2.RETURN)
        return PT
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
    
    def PrintHolding(self):
        for port in self.GroupPort:
            print(port.holding)
    
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
    def __init__(self,BegT,EndT,SR='ALL',GN=10,TF='W',BTF='D',IndNeu=False):
        self.BegT=BegT
        self.EndT=EndT
        self.StockRange=SR
        self.GroupNum=GN
        self.TradeFreq=TF
        self.BTFreq=BTF
        self.IndNeutral=IndNeu
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
    
    def SortNGroup(self,DFS):
        FDF=(DFS.sort_values(by='MF',ascending=False)).reset_index(drop=True)
        self.FDF=FDF.copy()
        length=len(FDF)
        """不分行业排序"""
        if self.IndNeutral==False:
            rank=pd.Series(range(0,length))
            group=rank*(self.GroupNum)/length
            FDF['GROUP']=group
            FDF['GROUP']=FDF['GROUP'].apply(lambda x:int(x)+1)

        """分行业内部排序，所有股票等权重"""
        if self.IndNeutral==True:
            FDF['RANK'] = FDF['MF'].groupby(FDF['SWLV1']).rank(ascending=False)
            FDF=FDF.sort_values(by=['SWLV1','RANK'])
            AA=FDF['RANK'].groupby(FDF['SWLV1']).max() 
            FDF['MAXRANK']=FDF['SWLV1'].apply(lambda x: AA[x])
            FDF['GROUP']=(FDF['RANK']-1)/FDF['MAXRANK']*(self.GroupNum)
            FDF['GROUP']=FDF['GROUP'].apply(lambda x:int(x)+1)
        """根据分组结果建立组合""" 
        
        GSL=[]
        for i in range(1,self.GroupNum+1):
            GSL.append([]) 
            SGDF=FDF.loc[FDF['GROUP']==i]
            print (SGDF)
            SL=list(SGDF.index)
            GSL[i-1]=Portfolio(SL)
        GP=GroupedPort(self.GroupNum,GSL)
        
        return GP
            
        
    
    def SFtoGroup(self,date):
        DF=GetFactorFF(date,'BP')
        #DF=self.GetFactors(date)
        DFS=FactorStandardize(DF)
        SortedDFS=self.SortNGroup(DFS)
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
    
    """回测主方法backtest()"""
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
        if self.BTFreq=='D':
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
        elif self.BTFreq=='W':
             for N in range(1,DL):
                if TDDF.iloc[N,1]==1:
                    date=TDDF.iloc[N,0]
                    ASDF=GetASDF(date)
                    #print(ASDF)
                    Ret=GP.GetReturns(ASDF)
                    RL=list(Ret.DAYRET)
                    RetTable.loc[date]=RL
                    print(date)
                    GP=self.SFtoGroup(date)                        
        #print(RetTable)
        return RetTable
starttime = datetime.now() 
print(starttime)
"""读取本地数据作为回测"""
global AllDR
global AllFactor   
#AllDR=LoadData('DayReturn0701-1803.csv')
#AllDR=LoadData('WeekReturn0701-1803.csv')
#AllFactor=LoadData('WeekFactor0701-1712.csv')

            
#参数设置

#设定回测起止日期
BegT=datetime(2007,1,1)
EndT=datetime(2017,12,31)


#设定股票选取范围(ALL,300,500,800)
StockRange='ALL'


"""设定行业中性及权重(False,True)"""
IndNeutral=True

"""设定分组数量"""
GroupNum=10

"""设定调仓频率('D','W','M')"""
TradeFreq='W'

"""设置回测频率（'D','W')"""
BTFreq='D'


#创建主程序
BT=SFBacktest(BegT,EndT,GN=GroupNum,TF=TradeFreq,BTF=BTFreq,IndNeu=IndNeutral)
#AAA=BT.SortNGroup(DFS)

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