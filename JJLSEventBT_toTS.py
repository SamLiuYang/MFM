# -*- coding: utf-8 -*-
"""
Created on Thu Mar 29 14:21:10 2018
分事件回测
@author: liuyang
"""
import datetime
import numpy as np
import pandas as pd
import tybutil as tyb
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
    #print (SCol)
    if (SCol['TRADABLE']==0) or (SCol['CLOSEZTDT']==-1):
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
    LSpair=[SCol.iloc[1],SCol.iloc[2],
            LCol.iloc[1],LCol.iloc[2]]
    return LSpair



#导入时间数据
print('Loading ShortStocks...')
#df0=pd.read_excel(r"D:\Sam\PYTHON\New=2.xlsx")
df0=pd.read_excel(r"D:\Sam\PYTHON\10-18解禁事件已筛选.xlsx")
EL=df0.loc[:,['代码','解禁日期','解禁比例','解禁类型']]


"""输入回测起止日期"""
BeginT=datetime.datetime(2018,1,1)   #输入回测起始日期
EndT=datetime.datetime(2018,4,27)    #输入回测截止日期(起始持仓日)

TDs=pd.read_csv(r"D:\Sam\PYTHON\Tradedates.csv",encoding='utf_8_sig',parse_dates=[0])
BTEL=EL[(EL['解禁日期']>=BeginT)&(EL['解禁日期']<=EndT)]    #预处理回测列表
BTTD=TDs[(TDs['TRADEDATE']>=BeginT)&(TDs['TRADEDATE']<=EndT)].loc[:,'TRADEDATE']    #预处理回测列表
TDList=TDs.loc[:,'TRADEDATE'].tolist()
BTTDList=BTTD.tolist()
Record=pd.DataFrame()

MFMHolding=tyb.GetMFfromWen()
#MFMHolding=pd.read_csv('MFMWen.csv',parse_dates=[0])
MFMaxTD=MFMHolding['date'].max()
conn=tyb.MYSQLEngine('tyb_stock')
for ed in BTTDList:
    #Dtd=DatetoDigit(td)
    DayEvent=BTEL[BTEL['解禁日期']==ed]
    if len(DayEvent)==0:
        pass
    else:
        edI=TDList.index(ed)    #解禁日在交易日列表中的序号
        if edI==False:
            edI=0
        else:
            tdI=edI-5           #建仓日为解禁日往前推5个交易日
            td=TDList[tdI]      #建仓日
            Dtd=DatetoDigit(td)
            Ded=DatetoDigit(ed)
            SQLstrHead1='select * from stockdaily_basic where TRADEDATE='
            #SQLstrHead2='select * from mfm_backtest_top_stocks where date='
            SQLstr1=SQLstrHead1+str(Dtd)
            #SQLstr2=SQLstrHead2+str(Dtd)
            SQLstr3=SQLstrHead1+str(Ded)
            AS=pd.read_sql(SQLstr1,conn)
            ASC=pd.read_sql(SQLstr3,conn)
            #MFS=sql.toDF(SQLstr2,'tyb_wen')
            if td<=MFMaxTD:
                MFS=MFMHolding[MFMHolding['date']==td].reset_index(drop=True)
            LS=pd.DataFrame()
            for i in MFS.index:
                #L=int(MFS.iloc[i,1][2:])
                L=MFS.iloc[i,1]
                LCol=AS[AS['STOCKID']==L]
                LS=LS.append(LCol)
            
            for N in range(len(DayEvent)):
                eve=DayEvent.iloc[N]
                StockPair=GetLongShort(eve,AS,LS)
                if StockPair==0:
                    continue
                SCode=StockPair[0]
                SName=StockPair[1]
                
                LCode=StockPair[2]
                LName=StockPair[3]
                """
                SCP=ASC[ASC['STOCKID']==SCode].iloc[0,16]
                LCP=ASC[ASC['STOCKID']==LCode].iloc[0,16]
                Sret=(SCP/SOP-1)*100
                Lret=(LCP/LOP-1)*100
                Pret=(Lret-Sret)*0.5
                """
                #print(SCode,Sret, LCode,Lret,Pret)
                list=[td,ed,SCode,SName,LCode,LName]
                PairRecord=pd.DataFrame([list],index=[N+1],
                                        columns=['起始日','解禁日','空代码','空名称',
                                                 '多代码','多名称'])
                if SCode!=LCode:
                    Record=Record.append(PairRecord)
    print(ed)

Record['解禁月份']=Record['解禁日'].apply(lambda x:(x.year*100+x.month))
Record.to_csv('BTEvent.csv',encoding='gbk',index=False)
    

        