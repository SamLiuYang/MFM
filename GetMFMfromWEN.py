# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 17:34:04 2018

@author: liuyang
"""

from datetime import datetime
import numpy as np
import pandas as pd
import MYSQLPD as sql


def GetMFMWen():
    SQLstr2='select * from mfm_backtest_top_stocks'
    AS=sql.toDF(SQLstr2,'tyb_wen')
    AS['code']=AS['code'].apply(lambda x: int(x[2:]))
    ASOUT=AS[['date','code']]
    return ASOUT


