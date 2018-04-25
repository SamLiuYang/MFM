# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 17:34:04 2018

@author: liuyang
"""

from datetime import datetime
import numpy as np
import pandas as pd
import pymysql
from sqlalchemy import create_engine


def MYSQLEngine(dbname):
    try:  
        engine = create_engine(
            "mysql+pymysql://root:Kxbs2018!@localhost:3306/"
            +dbname+"?charset=utf8") 
    except sqlalchemy.exc.OperationalError as e:  
        print('Error is '+str(e))  
        sys.exit()  
    except sqlalchemy.exc.InternalError as e:  
        print('Error is '+str(e))  
        sys.exit()
    return engine

def GetMFfromWen():
    SQLstr='select * from mfm_backtest_top_stocks'
    conn=MYSQLEngine('tyb_wen')
    AS=pd.read_sql(SQLstr,conn)
    AS['code']=AS['code'].apply(lambda x: int(x[2:]))
    AS['date']=pd.to_datetime(AS.date)
    ASOUT=AS[['date','code']]
    return ASOUT

#df=GetMFfromWen()
#A=GetMFHWen()
#A.to_csv('MFMWen.csv',encoding='utf_8_sig',index=False)


