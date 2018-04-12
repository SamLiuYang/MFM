# -*- coding: utf-8 -*-
"""
Created on Thu Mar 22 09:16:11 2018

@author: liuyang
"""
import sys
sys.path.append("D:\Program Files\Tinysoft\Analyse.NET")#
import TSLPy3 as tp #导入模块
import pandas as pd

def tsbytestostr(data):
    if (isinstance(data,(tuple)) or isinstance(data,(list))):
        lendata = len(data)
        ret = []
        for i in range(lendata):
            ret.append(tsbytestostr(data[i]))
    elif isinstance(data,(dict)):
        lendata = len(data)
        ret ={}
        for i in data:
            ret[tsbytestostr(i)] = tsbytestostr(data[i])
    elif isinstance(data,(bytes)):
        ret = data.decode('gbk')
    else:
        ret = data
    return ret

def CallTSFunc(FuncName,FuncParam=[],SysParam={},outtype='list'):

    data=tp.RemoteCallFunc(FuncName,FuncParam,SysParam)
    
    #a=tp.RemoteCallFunc("OneDayStocks",[tp.EncodeDate(2010,1,4)],{})
    a=tsbytestostr(data[1])
    df=pd.DataFrame(a)
    if outtype=='list':
        return a
    elif outtype=='DF':
        return df
        

def TSDate(Y,M,D):
    d=tp.EncodeDate(Y,M,D)
    return d

#date=TSDate(2010,1,4)

#a=CallTSFunc("OneDayStocks",[date],{})