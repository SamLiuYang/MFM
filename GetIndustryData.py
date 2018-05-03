# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 11:28:52 2018

@author: liuyang
"""

import tyb_util as tyb
import pandas as pd
import numpy as np
from datetime import datetime

#conn=tyb.create_mysql_engine('tyb_stock')

begT=datetime(2014,2,21)    #新申万一级分类起始日
endT=datetime(2018,4,27)

#begT=datetime(2007,1,1)    
#endT=datetime(2014,2,20)    #旧申万一级分类截止日

starttime = datetime.now() 

dates=tyb.get_tradedates(begT,endT,Freq='D')
#sw1_list=list(range(101,124,1))    #老申万一级
sw1_list=list(range(201,229,1))
record=[]
for dd in dates:
    print(dd)
    DF=tyb.get_stockdaily_basic(dd)

    AS300=DF[DF['PCT50']>0]
    sw1=AS300[['DAYRETURN','SWLV1','PCT50']].groupby(['SWLV1'])
    ind_ret=sw1.apply(lambda x: np.average(x['DAYRETURN'],weights=x['PCT50']))
    ind_count=sw1['PCT50'].count()
    ind_weight=sw1['PCT50'].sum()
    
    sw1_day=[[dd,ind,ind_count[ind],ind_weight[ind],ind_ret[ind]]
                if (ind in ind_count.index)
                else [dd,ind,0,0,0]
                for ind in sw1_list]
    record.extend(sw1_day)
    
RDF=pd.DataFrame(record,columns=['DATE','SWLV1','COUNT','WEIGHT','DAYRET'])

#RDF.to_csv('300IndOld.csv', encoding='utf_8_sig',index=False)


stoptime = datetime.now() 
print(stoptime-starttime)