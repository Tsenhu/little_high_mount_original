# -*- coding: utf-8 -*-
"""
Created on Sun Jul 30 21:50:09 2023

@author: tsenh
"""
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 28 20:31:35 2022

@author: tsenh
"""
import urllib.request
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import datetime as dt
from datetime import datetime, timedelta
#import talib as ta
import numpy as np
import os, os.path 
from collections import OrderedDict
import re
import bs4
import requests
import time as t
from dateutil.relativedelta import relativedelta
import pandas_datareader.data as web
from urllib.parse import quote_plus as urlquote
import yahoo_fin.stock_info as si
import yfinance as yf


t_ini = t.time()
_host = '127.0.0.1'
_db = 'awesome'
_user = 'root'
_password = 'Albert@25'
engine = create_engine('mysql://'+_user+':'+urlquote(_password)+'@'+_host)


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
        


weekly_update =  read_query(engine, "select * from awesome.stock_er_weekly_update where er_date<curdate()")

er_close = []
er_volume = []
er_next_close = []
er_next_volume = []
for i in range(len(weekly_update)):
    t0 =  t.time()
    temp_ticker = weekly_update['ticker'][i]
    er_date = weekly_update['er_date'][i]
    
    start_date = er_date 
    end_date = er_date + timedelta(days=10)
    
    try:
        temp_price = yf.Ticker(temp_ticker).history(start=start_date, end=end_date).iloc[:2].reset_index()
        er_close.append(temp_price['Close'][0])
        er_volume.append(temp_price['Volume'][0])
        er_next_close.append(temp_price['Close'][1])
        er_next_volume.append(temp_price['Volume'][1])
        print('{0} {1} takes {2} seconds'.format(temp_ticker, str(er_date) , t.time()-t0))
    except:
        temp_price = pd.DataFrame()
        er_close.append(np.nan)
        er_volume.append(np.nan)
        er_next_close.append(np.nan)
        er_next_volume.append(np.nan)

    
price_info = pd.DataFrame(list(zip(er_close, er_volume, er_next_close, er_next_volume)), 
                          columns = ['er_close', 'er_volume', 'er_next_close', 'er_next_volume'])
    
backtest= pd.concat([weekly_update, price_info], axis =1)

backtest['win_or_lose'] = backtest.apply(lambda x: 1 if x['er_close']<x['er_next_close'] else 0, axis=1 )