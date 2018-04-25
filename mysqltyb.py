# -*- coding: utf-8 -*-
"""
Created on Tue Apr 24 08:49:58 2018

@author: liuyang
"""
import datetime
import pandas as pd
import pymysql
from sqlalchemy import create_engine

def set_engine(dbname):
    
    engine = create_engine(
            "mysql+pymysql://root:Kxbs2018!@localhost:3306/"
            +dbname+"?charset=utf8")
    return engine

conn=set_engine('tyb_wen')

df=pd.read_csv('NewXSGJJ.csv')

#df['jj_date']=df['jj_date'].apply(lambda x: datetime.date(x.year,x.month,x.day))
df.to_sql('xsgjj',con=conn,index=False,if_exists='replace')


