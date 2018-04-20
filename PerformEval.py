# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 11:32:58 2018

@author: liuyang
"""
import numpy as np
import pandas as pd
import time


class NAV_PA(object):
    """RET模式下输入列数为1的DF，第一列为日期，第二列为当期收益率"""
    def __init__(self, input,inputmode='RET%',interval='D'):
        self.interval=interval
        length=len(input)
        self.length=length
        if __name__ == "__main__":
            print ('Loading NAV series...')
        if inputmode=='RET%':
            self.RET=input
            NAV=pd.DataFrame([[input.iloc[0,0],1]],index=[0],columns=['Date','NAV'])
            length=len(input)
            for N in range(1,length):
                d=input.iloc[N,0]
                v=NAV.iloc[N-1,1]*(1+0.01*input.iloc[N-1,1])
                NAV.loc[N]=[d,v]
            self.NAV=NAV
        elif inputmode=='NAV':
            #self.NAV=pd.DataFrame(input,columns=['Date','NAV'])
            input.columns=['Date','NAV']
            self.NAV=input
            RET=pd.DataFrame(columns=['Date','NEXTRET'])
            
            for N in range(0,length-1):
                d=input.iat[N,0]
                r=(input.iat[N+1,1]/input.iat[N,1]-1)*100
                RET.loc[N]=[d,r]
            RET.loc[length-1]=[input.iat[length-1,0],0]

            self.RET=RET
        RN=pd.merge(self.RET,self.NAV,on='Date')
        RN['Date'] = pd.to_datetime(RN['Date'])
        self.RETNAV=RN
        if __name__ == "__main__":
            print ('Done')
    
    def Drawdown(self):
        if __name__ == "__main__":
            print ("Calculating DD:")
        NavS=self.NAV
        DD=pd.DataFrame([[NavS.iat[0,0],NavS.iat[0,1],0,0,0]],index=[0],columns=['Date','NAV','Drawdown','BeginI','CDD'])
        MP=NavS.iat[0,1]
        MPI=0
        CDD=0
        L=self.length
        for N in range(1,L):
                date=NavS.iat[N,0]
                P=NavS.iat[N,1]
                ddv=min(P/MP-1,0)
                if NavS.iat[N,1]>MP:
                   MP=P
                   MPI=N
                   CDD=0
                else:
                    CDD+=1
                DD.loc[N]=[date,P,ddv,MPI,CDD]
        self.DD=DD
        
        MDD=DD[DD['Drawdown']==DD.Drawdown.min()]
        MDDS=MDD.iloc[0:1]
        if __name__ == "__main__":
            print("Done")
        MaxDD=MDDS.iat[0,2]*100
        MDBegin=DD.iat[MDDS.iloc[0,3],0]
        MDDate=MDDS.iat[0,0]
        #MDTD=MDDS.iat[0,4]
        LDD=DD[DD['CDD']==DD.CDD.max()]
        LDDS=LDD.iloc[0:1]
        LDBegin=DD.iat[LDDS.iat[0,3],0]
        LDEnd=LDDS.iat[0,0]
        RecTD=LDDS.iat[0,4]

        MDOut=pd.Series([MaxDD,MDBegin,MDDate,RecTD,LDBegin,LDEnd,],
                        index=['最大回撤','最大回撤起始日','最大回撤发生日','最长回撤交易日数',
                               '最长回撤起始日','最长回撤结束日'])
        self.MDOut=MDOut
        return MDOut
    def Performance(self,r=0):
        RN=self.RETNAV
        t0=RN.iat[0,0]
        t1=RN.iloc[-1,0]
        
        TRet=(RN.iloc[-1,2]/RN.iloc[0,2]-1)*100
        self.TRet=TRet
        
        TD=RN.iloc[-1,0]-RN.iloc[0,0]
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
        PFM=self.Performance()
        DD=self.Drawdown()
        PFM_DD=pd.concat([self.PFOut,self.MDOut])
        self.PFM_DD=PFM_DD
        #print (PFM_DD)
        return PFM_DD
    
    def Split_Year(self):
        RN=self.RETNAV
        t0=RN.iat[0,0]
        t1=RN.iloc[-1,0]
        Y0=t0.year
        Y1=t1.year
        print (Y0,Y1)
        RN['Year']=RN['Date'].apply(lambda x: x.year)
        #print (RN)
        #SplitY=pd.DataFrame()
        RNS=NAV_PA(RN,inputmode='RET%')
        #r=RNS.Perf_Split()
        r=RNS.PFM_DD()
        SplitY=pd.DataFrame([r])
        SplitY.insert(0,'年份','全部')
        if Y0==Y1:
            SplitY.iloc[0,0]=Y0
            return SplitY
            pass
            #RN0=NAV_PA(RN,inputmode='RET%')            
        else:
            i0=0
            i1=0
            for Y in range(Y0,Y1+1):
                i1=RN[RN['Year']==Y].index.max()
                RNY=RN.loc[i0:(i1)]
                ypa=NAV_PA(RNY,inputmode='RET%')
                #r=ypa.Perf_Split()
                r=ypa.PFM_DD()
                YDF=pd.DataFrame([r])
                YDF.insert(0,'年份',Y)
                SplitY=SplitY.append(YDF)
                i0=i1
                print ('Calculating ',Y)
                #print (YDF)
            return SplitY
