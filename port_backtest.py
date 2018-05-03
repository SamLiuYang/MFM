# -*- coding: utf-8 -*-
"""
Created on Fri Mar 23 09:02:02 2018

@author: liuyang
"""
from datetime import datetime
import numpy as np
import pandas as pd
import tyb_util as tyb
import PerformEval as pe



class Portfolio(object):
    """
    Portfolio类,创建时输入stock_list,以及等长的weight_list,若weight_list为空,
    则认为stock_list为等权
    属性：stocklist,weightlist,portlist(返回(stockid,weight)的列表),
    len,下标可用
    """
    
    def __init__(self,stock_list,weight_list=False,tradedate=0):
        l=len(stock_list)
        self.tradedate=tradedate
        if l==0:
            self.stocklist=[0]
            self.weightlist=[1]
        else:
            self.stocklist=stock_list
            if weight_list==False:
                ew=1/l
                self.weightlist=[ew for i in range(0,l)]
            elif len(weight_list)==l:
                self.weightlist=weight_list
            else:
                raise Exception('weight_list length ERROR')
        self.portlist=[(s,w) for (s,w) in zip(self.stocklist,self.weightlist) ]
                
    def  __len__(self):
        return len(self.stocklist)
    
    def __repr__(self):
        return 'Portfolio with %d stocks on %s' % (len(self.stocklist),self.tradedate)

    def __getitem__(self, n):
        return self.portlist[n]

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
    DRDF=DRDF.set_index(['TRADEDATE','STOCKID'])
    print('Done')
    return DRDF
    
def get_day_return(date,port):
    """输入日期及当日持仓数据,
    返回（持仓记录，当日收益率）
    主程序必须加载股票每日收益清单 ALLDR"""
    global AllDR
    DRDF=AllDR.loc[date][['RETURN']]
    
    if isinstance(port,Portfolio)==False:
        raise Exception('Portfolio input Error')
    else:
        PTDF=pd.DataFrame(port.portlist,columns=["STOCKID","WEIGHT"])
        PTDF=PTDF.set_index('STOCKID')
    #port_ret=pd.concat([PTDF,DRDF],join='left',axis=1)
    PRDF=PTDF.merge(DRDF,left_index=True,right_index=True)
    port_ret=sum(PRDF.WEIGHT*PRDF.RETURN)
    REC=PRDF.reset_index()

    REC.insert(0, 'TRADEDATE', date)
    return REC,port_ret

def holding_bt(EndT,input_h,BegT=0,ignore_date=True):
    """输入持仓数据（包含'DATE','STOCKID','WEIGHT'),
    返回(RETNAV序列，收益分析，每日持仓清单)
    主程序必须加载股票每日收益清单 ALLDR"""
    initial_date=input_h['DATE'].min()        
    tds=tyb.get_tradedates(initial_date,EndT,Freq='D')
    h=input_h[input_h['DATE']==initial_date]
    
    P=Portfolio(list(h['STOCKID']),list(h['WEIGHT']),initial_date)
    ret_list=[0]
    record_df=pd.DataFrame()
    dl=len(tds)
    for N in range(1,dl):
        td=tds[N]
        print(td)
        h=input_h[input_h['DATE']==td]        
        Rec,ret=get_day_return(td,P)
        ret_list.extend([ret])
        record_df=record_df.append(Rec)
        if len(h)!=0:
            P=Portfolio(list(h['STOCKID']),list(h['WEIGHT']),td)
    port_ret=pd.DataFrame({'TRADEDATE':tds,"RETURN":ret_list})
    port_ret=port_ret[['TRADEDATE','RETURN',]]
    pa_input=port_ret.set_index(['TRADEDATE'])
    bt=pe.PA(pa_input,inputmode='RET')
    ret_nav=bt.RETNAV
    analysis=bt.PFM_DD()
    
    
    return (ret_nav,analysis,record_df) 
            
           
global AllDR
 
#AllDR=LoadData('DayReturn0701-1803.csv')
EndT=datetime(2014,3,31)
input_h=pd.read_csv('input/input_test.csv',header=0,index_col=False,parse_dates=[0])

AAA=holding_bt(EndT,input_h)        
    


