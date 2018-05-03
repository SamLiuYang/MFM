# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 11:32:58 2018

@author: liuyang
"""
import numpy as np
import pandas as pd
from datetime import datetime


class PA(object):
    """RET模式下输入列数为1的DF，第一列为日期，第二列为当期收益率"""
    def __init__(self, input,inputmode='RET',interval='D'):
        self.interval=interval
        length=len(input)
        self.length=length
        if __name__ == "__main__":
            print ('Loading NAV series...')
        if inputmode=='RET':
            input.columns=['RET']
            self.RET=input
            Temp=self.RET.copy()
            RETList=list(input['RET'])
            self.RETList=RETList
            nav=1
            NAVList=[]
            for i,ret in enumerate(RETList):
                nav=nav*(1+ret*0.01)
                NAVList.append(nav)
            Temp['NAV']=NAVList
            self.RETNAV=Temp
            self.NAV=Temp[['NAV']]
                
        elif inputmode=='NAV':
            input.columns=['NAV']
            self.NAV=input
            Temp=self.NAV.copy()
            Temp['RET']=(Temp['NAV']/Temp['NAV'].shift(1)-1)*100
            Temp=Temp[['RET','NAV']]
            Temp.iloc[0,0]=0
            self.RETNAV=Temp
            self.RET=Temp[['RET']]
        if __name__ == "__main__":
            print ('Done')
    def drawdown(self):
        if __name__ == "__main__":
            print ("Calculating DD:")
        Temp=self.NAV.copy().reset_index()
        Temp.columns=['DATE','NAV']
        Temp['NAVMAX']=Temp['NAV'].cummax()
        Temp['DD']=(Temp['NAV']/Temp['NAVMAX']-1)*100
        Temp['MAXDD']=Temp['DD'].cummin()
        DDlist=list(Temp['DD'])
        CDlist=[]
        for i,x in enumerate(DDlist):
            if x==0:
                cd=0
            else:
                cd=cd+1
            CDlist.append(cd)        
        Temp['ContDD']=CDlist
        
        MaxDD=Temp['MAXDD'].iloc[-1]
        MDOI=Temp[Temp['MAXDD']==MaxDD].index[0]
        #print(Temp)                                             #测试
        MDOccur=Temp.loc[MDOI,'DATE']
        MDBI=MDOI-Temp.loc[MDOI,'ContDD']
        MDBegin=Temp.loc[MDBI,'DATE']
        LDD=Temp['ContDD'].max()
        LDI=Temp[Temp['ContDD']==LDD].index[0]
        LDEI=min(LDI+1,self.length-1)
        LDBI=LDI-LDD
        LDBegin=Temp.loc[LDBI,'DATE']
        LDEnd=Temp.loc[LDEI,'DATE']

        MDOut=pd.Series([MaxDD,MDBegin,MDOccur,LDD,LDBegin,LDEnd],
                        index=['最大回撤','最大回撤起始日','最大回撤发生日','最长回撤交易日数',
                               '最长回撤起始日','最长回撤结束日'])
        self.MDOut=MDOut
        return MDOut

    def performance(self,r=0):
        RN=self.RETNAV
        t0=RN.index[0]
        t1=RN.index[-1]
        TRet=(RN.iloc[-1,1]/RN.iloc[0,1]-1)*100
        self.TRet=TRet
        
        TD=t1-t0
        TY=TD.days/365
        self.TY=TY
        ARet=((1+TRet/100)**(1/TY)-1)*100
        self.ARet=ARet
        
        SD=RN.RET.std()
        if self.interval=='D':
            ASD=SD*np.sqrt(245)
        elif self.interval=='W':
            ASD=SD*np.sqrt(52)
        elif self.interval=='M':
            ASD=SD*np.sqrt(12)
        self.ASD=ASD
        SR=(ARet-r)/ASD
        self.SR=SR
        WR=RN[RN['RET']>0].count().RET/RN.count().RET
        LR=RN[RN['RET']<0].count().RET/RN.count().RET
        PFOut=pd.Series([t0,t1,TRet,TY,ARet,ASD,SR,WR,LR],
                        index=['起始日','截止日','区间收益率%','回测时长（年）',
                               '年化收益率%','年化波动率%','夏普比率','胜率','亏损率'])
        self.PFOut=PFOut

        return PFOut
    
    def Perf_Split(self):
        RN=self.RETNAV
        t0=RN.iat[0,0]
        t1=RN.iloc[-1,0]
        TRet=(RN.iloc[-1,2]/RN.iloc[0,2]-1)*100
        self.TRet=TRet
        
        TD=RN.iloc[-1,0]-RN.iloc[0,0]
        TY=TD.days/365
        self.TY=TY
        WR=RN[RN['RET']>0].count().RET/RN.count().RET
        LR=RN[RN['RET']<0].count().RET/RN.count().RET
        SOut=pd.Series([t0,t1,TRet,WR,LR],
                        index=['起始日','截止日','区间收益率%','胜率','亏损率'])
        self.SOut=SOut
        DD=self.Drawdown()
        PS=pd.concat([self.SOut,self.MDOut])
        return PS
    
    def PFM_DD(self,DateType='str',r=0):
        PFM=self.performance()
        DD=self.drawdown()
        PFM_DD=pd.concat([self.PFOut,self.MDOut])
        self.PFM_DD=PFM_DD
        #print (PFM_DD)
        return PFM_DD
    
    def Split_Year(self):
        RN=self.RETNAV
        t0=RN.index[0]
        t1=RN.index[-1]
        #return t0
        Y0=t0.year
        Y1=t1.year
        r=self.PFM_DD
        RN=RN.copy().reset_index()
        RN=RN.rename(columns={"index": "DATE"})
        RN['YEAR']=RN['DATE'].apply(lambda x: x.year)
        #r=RNS.Perf_Split()
        
        SplitY=pd.DataFrame([r])
        SplitY.insert(0,'年份','全部')

        if Y0==Y1:
            SplitY.iloc[0,0]=Y0
            return SplitY
            pass         
        else:
            i0=0
            i1=0
            for Y in range(Y0,Y1+1):
                i1=RN[RN['YEAR']==Y].index[-1]
                RNYI=RN.iloc[i0:i1]
                i1=RN[RN['YEAR']==Y].index.max()
                RNY=RN.loc[i0:(i1)]
                RETY=RNY[['DATE','RET']].set_index('DATE')
                RETY.iat[0,0]=0
                ypa=PA(RETY,inputmode='RET')
                #r=ypa.Perf_Split()
                r=ypa.PFM_DD()
                YDF=pd.DataFrame([r])
                YDF.insert(0,'年份',Y)
                SplitY=SplitY.append(YDF)
                i0=i1
                #print ('Calculating ',Y)
                #print (YDF)
            return SplitY

"""回测案例
starttime = datetime.now() 

RET=pd.read_csv('RET1.csv',index_col=0,parse_dates=True,header=None,names=['RET'])
NAV=pd.read_csv('NAV1.csv',index_col=0,parse_dates=True,header=None,names=['NAV'])

#AAA=PA(RET)
BBB=PA(RET,inputmode='RET')
#C=BBB.drawdown()
#D=BBB.Performance()
E=BBB.PFM_DD()
F=BBB.Split_Year()

stoptime = datetime.now() 

print(stoptime-starttime)
"""