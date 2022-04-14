# -*- coding: utf-8 -*-
"""
Created on Wed Apr 13 21:10:43 2022

@author: tsenh

Business should be free. No political affect. Any companies want to play political game, I will avoid investing!

Level of company:
    A: totally ban
    B: totally ban
    C: partically ban
    D: Invest
    F: Invest
"""
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import datetime as dt
from datetime import datetime, timedelta
#import talib as ta
import numpy as np
import os, os.path 
from collections import OrderedDict
import time as t
from urllib.parse import quote_plus as urlquote
from functools import reduce


t_ini = t.time()
_host = '127.0.0.1'
_db = 'awesome'
_user = 'root'
_password = 'Albert@25'
engine = create_engine('mysql://'+_user+':'+urlquote(_password)+'@'+_host)
open_path = 'C:/Users/tsenh/GitHub/little_high_mount_original/nasdaq_list/'

def read_query(engine, query):
        con = engine.connect()
        #print("connection = ", con)
        try:
            #print('query running')
            results = con.execute(query)
            names = results.keys()
            #print('creating df')
            df = pd.DataFrame(results.fetchall(), columns=names)
            return df
        finally:
            if con is not None:
                con.close()

def delete_action(engine, query):
        con = engine.connect()
        
        con.execute(query)
        
        con.close()
        
        print('delete table action finish')


data_raw = pd.read_excel(open_path+'Expanded List - Apr 6 v5.xlsx', header = 3, usecols ='A:G')

data_f = data_raw.iloc[0:131]
data_f['level'] = 'F'
data_f['comment'] = 'investable'

data_d = data_raw.iloc[167:258]
data_d['level'] = 'D'
data_d['comment'] = 'investable'

data_c = data_raw.iloc[280:342]
data_c['level'] = 'C'
data_c['comment'] = 'half investable'

data_b = data_raw.iloc[380:617]
data_b['level'] = 'B'
data_b['comment'] = 'quarter investable'

data_a = data_raw.iloc[641:893]
data_a['level'] = 'A'
data_a['comment'] = 'no investable'

data_list =  [data_a, data_b, data_c, data_d, data_f]

df_final = pd.concat(data_list).reset_index(drop=True)
df_final = df_final.rename(columns = {'Name':'Company', 'Action ':'Action', 'Link to Announcement':'Action_Details',
                                      'Magnitude of Russian Operations':'Affects',  'Stock Ticker':'Ticker'})
df_final.to_sql(name='company_freedom', con=engine, schema = 'awesome', if_exists='replace', index = False)
