# -*- coding: utf-8 -*-
"""
Created on Tue Aug  4 20:42:45 2020

@author: tsenh
"""

import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import datetime as dt
from datetime import datetime, timedelta
#import talib as ta
import numpy as np
import os, os.path 
#import tensorflow as tf
#from tensorflow.contrib import rnn
from collections import OrderedDict
import re
import bs4
import requests
import time as t
from dateutil.relativedelta import relativedelta
import pandas_datareader.data as web
import yfinance as yf
from urllib.parse import quote_plus as urlquote

t_ini = t.time()
_host = '127.0.0.1'
_db = 'awesome'
_user = 'root'
_password = 'Albert@25'
engine = create_engine('mysql://'+_user+':'+urlquote(_password)+'@'+_host)

#_table = 'stock.ta_data'

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

parent_path = 'c:/Users/tsenh/github/little_high_mount_original/'

ticker  = read_query(engine, 'select distinct ticker from awesome.hist_er')
'''
ticker_info  = read_query(engine, 
                          'select a.ticker, a.date from( \
                              select ticker, date, rank() over (partition by ticker order by date desc) as ranked from awesome.hist_er) as a\
                            where a.ranked <=2\
                            order by a.ticker, a.date')

'''


sector = []
industry = []
country = []
state = []
city = []
company_name = []
tt = t.time()
for i in range(len(ticker)):
    t0 =  t.time()
    t_ticker = yf.Ticker(ticker['ticker'][i])
    try:
        sector.append(t_ticker.info['sector'])
    except:
        sector.append(np.nan)
    try:
        industry.append(t_ticker.info['industry'])
    except:
        industry.append(np.nan)
    try:
        country.append(t_ticker.info['country'])
    except:
        country.append(np.nan)
    try:
        state.append(t_ticker.info['state'])
    except:
        state.append(np.nan)
    try:
        city.append(t_ticker.info['city'])
    except:
        city.append(np.nan)
    try:
        company_name.append(t_ticker.info['longName'])
    except:  
        company_name.append(np.nan)
    print('{0} takes {1} seconds'.format(ticker['ticker'][i] , t.time()-t0))
    t.sleep(1)
print('All takes {0} seconds'.format(t.time()-tt))

ticker['sector'] = sector
ticker['industry'] = industry
ticker['country'] = country
ticker['state'] = state
ticker['city'] = city
ticker['company_name'] = company_name

ticker1= ticker.replace(np.nan, '', regex=True)



ticker1.to_sql(name='company_info', con=engine, schema = 'awesome', if_exists='replace', index = False)