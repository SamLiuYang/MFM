# -*- coding: utf-8 -*-
"""
Created on Thu Mar 29 14:21:10 2018
分事件回测
@author: liuyang
"""
import datetime
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

def DatetoDigit(date):
    digitdate=date.year*10000+date.month*100+date.day
    return digitdate

def GetLongShort(event,AS,LS,pairmode='MV'):    
    #输入解禁建仓股票信息，建仓日全市场股票信息，所有多因子股票信息，返回匹配股票

    S=int((event['代码'])[0:6])
    SCol=AS[AS['STOCKID']==S]
    SCol=SCol.iloc[0]
    if SCol['TRADABLE']==0:
        return 0
    SInd=SCol['SWLV1']
    SMV=SCol['MARKETVALUE']
    LC=LS[LS['SWLV1']==SInd]
    LC=LC[LC['TRADABLE']==1]
    if len(LC)==0:
        return 0
    LC['MVDiff']=LC['MARKETVALUE'].map(lambda x: abs(x-SMV))
    if pairmode=='MV':
        LC=LC.sort_values(by='MVDiff',ascending=True)
        LCol=LC.iloc[0]#最接近流通市值匹配
    #返回股票代码，名称
    #LSpair=[SCol,LCol]
    LSpair=[SCol.iloc[1],SCol.iloc[2],SCol.iloc[16],
            LCol.iloc[1],LCol.iloc[2],LCol.iloc[16]]
    return LSpair



#导入时间数据
print('Loading ShortStocks...')
#df0=pd.read_excel(r"D:\Sam\PYTHON\New=2.xlsx")
df0=pd.read_excel(r"D:\Sam\PYTHON\10-18解禁事件已筛选.xlsx")
EL=df0.loc[:,['代码','解禁日期','解禁比例','解禁类型']]

BeginT=datetime.datetime(2018,1,1)   #输入回测起始日期
EndT=datetime.datetime(2018,3,29)    #输入回测截止日期(起始持仓日)

TDs=pd.read_csv(r"D:\Sam\PYTHON\Tradedates.csv",encoding='utf_8_sig',parse_dates=[0])
BTEL=EL[(EL['解禁日期']>=BeginT)&(EL['解禁日期']<=EndT)]    #预处理回测列表
BTTD=TDs[(TDs['TRADEDATE']>=BeginT)&(TDs['TRADEDATE']<=EndT)].loc[:,'TRADEDATE']    #预处理回测列表
TDList=TDs.loc[:,'TRADEDATE'].tolist()
BTTDList=BTTD.tolist()
Record=pd.DataFrame()
C=0
for ed in BTTDList:
    #Dtd=DatetoDigit(td)
    DayEvent=BTEL[BTEL['解禁日期']==ed]
    if len(DayEvent)==0:
        pass
    else:
        edI=TDList.index(ed)
        if edI==False:
            edI=0
        else:
            tdI=edI-5
            td=TDList[tdI]
            Dtd=DatetoDigit(td)
            Ded=DatetoDigit(ed)
            SQLstrHead1='select * from stockdaily_basic where TRADEDATE='
            SQLstrHead2='select * from mfm_backtest_top_stocks where date='
            SQLstr1=SQLstrHead1+str(Dtd)
            SQLstr2=SQLstrHead2+str(Dtd)
            SQLstr3=SQLstrHead1+str(Ded)
            AS=sql.toDF(SQLstr1,'tyb_stock')
            ASC=sql.toDF(SQLstr3,'tyb_stock')
            MFS=sql.toDF(SQLstr2,'tyb_wen')
            LS=pd.DataFrame()
            for i in MFS.index:
                L=int(MFS.iloc[i,1][2:])
                LCol=AS[AS['STOCKID']==L]
                LS=LS.append(LCol)
            
            for N in range(len(DayEvent)):
                eve=DayEvent.iloc[N]
                StockPair=GetLongShort(eve,AS,LS)
                if StockPair==0:
                    continue
                SCode=StockPair[0]
                SName=StockPair[1]
                SOP=StockPair[2]
                LCode=StockPair[3]
                LName=StockPair[4]
                LOP=StockPair[5]
                SCP=ASC[ASC['STOCKID']==SCode].iloc[0,16]
                LCP=ASC[ASC['STOCKID']==LCode].iloc[0,16]
                Sret=(SCP/SOP-1)*100
                Lret=(LCP/LOP-1)*100
                Pret=(Lret-Sret)*0.5
                #print(SCode,Sret, LCode,Lret,Pret)
                list=[td,ed,SCode,SName,Sret,LCode,LName,Lret,Pret]
                PairRecord=pd.DataFrame([list],index=[N+1],
                                        columns=['起始日','解禁日','空代码','空名称','空收益率',
                                                 '多代码','多名称','多收益率','多-空收益'])
                if SCode!=LCode:
                    Record=Record.append(PairRecord)
    print(ed)
    #C=C+1
    #print (C)
Record.to_csv('BTEvent.csv',encoding='utf_8_sig')
    
a=Record[['解禁日','多-空收益']].groupby(['解禁日'],axis=0).mean()
R=Record.copy()
R['月份']=R['解禁日'].apply(lambda x: x.year*100+x.month)
b=R[['月份','多-空收益']].groupby(['月份'],axis=0).mean()
avgret=b.reset_index()
c=R[['月份','多-空收益']].groupby(['月份'],axis=0).count()
count=c.reset_index().copy()
count.rename(columns={'多-空收益':'事件数量'}, inplace = True)
d=pd.merge(avgret,count,on='月份')

        