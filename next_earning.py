# -*- coding: utf-8 -*-
"""
Created on Thu Jan  6 14:52:44 2022

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
# This funtion scrap each Symbol page and extract the Zacks Rank
def zacks_rank(Symbol):
 
    # Wait for 2 seconds
    #time.sleep(2)
    url = 'https://quote-feed.zacks.com/index?t='+Symbol
    downloaded_data  = urllib.request.urlopen(url)
    data = downloaded_data.read()
    data_str = data.decode()
    Z_Rank =["Strong Buy","Buy","Hold","Strong Sell", "Sell"]

    for Rank in Z_Rank:
       #data_str.find(Rank)# az tooye list Z_Rank doone doone check kon va yeki ra dar str_data
       # peyda kon ;; faghat index harf aval ro retrun mikond
       if(data_str.find(Rank) != -1):
           return Rank #data_str[res:res+len(Rank)]#

ticker  = read_query(engine, 'SELECT distinct ticker FROM awesome.hist_er_elite where date_add(date, interval 90 day)> sysdate()')

date = []
zack_rank = []
zack_dic = {'Strong Buy':1, 'Buy':2, 'Hold':3, 'Sell':4, 'Strong Sell':5}

tt = t.time()
for i in range(len(ticker)):
    t0 =  t.time()
    try:
        date.append(si.get_next_earnings_date(ticker['ticker'][i]).date())
    except:
        date.append(np.nan)
        print('{0} cannot make it'.format(ticker['ticker'][i]))
    try:
        zack_rank.append(zacks_rank(ticker['ticker'][i]))
        print('{0} takes {1} seconds'.format(ticker['ticker'][i] , t.time()-t0))
    except:
        zack_rank.append('')
        print('{0} has no zack info'.format(ticker['ticker'][i]))
print('All takes {0} seconds'.format(t.time()-tt))

ticker['er_date'] = date
ticker['zack_rank'] = zack_rank


temp_elite_ticker = ticker[ticker['er_date']> datetime.now().date()]
temp_elite_ticker.to_sql(name='next_er_date', con=engine, schema = 'awesome', if_exists='replace', index = False)

