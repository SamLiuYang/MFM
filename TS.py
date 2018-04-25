# -*- coding: utf-8 -*-
"""
Created on Thu Mar 22 09:16:11 2018

@author: liuyang
"""
import sys
sys.path.append("D:\Program Files\Tinysoft\Analyse.NET")#
import TSLPy3 as tp #导入模块
import pandas as pd
from datetime import datetime

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
    a=tsbytestostr(data[1])    
    if outtype=='list':
        return a
    elif outtype=='df':
        df=pd.DataFrame(a)
        return df      

def TSDate(Y,M,D):
    d=tp.EncodeDate(Y,M,D)
    return d

starttime = datetime.now() 

date1=TSDate(2018,4,19)
date2=TSDate(2018,4,20)

#data=tp.RemoteCallFunc("OneDayIndex",[date],{})
a=CallTSFunc("OneDayIndex",[date1],{},outtype='list')
print (a)
b=CallTSFunc("OneDayIndex",[date2],{},outtype='list')
a.extend(b)
stoptime = datetime.now() 
print(stoptime-starttime)