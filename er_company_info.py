# -*- coding: utf-8 -*-
"""
Created on Tue Aug  4 20:42:45 2020

@author: tsenh
"""

"""
To Do list

It takes 20h to finish the process. Need to split the process into 2 or 3 process by companies
"""
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import datetime as dt
from datetime import datetime, timedelta
import urllib.request
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
import pandas_datareader
import yfinance as yf
from urllib.parse import quote_plus as urlquote
import re
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
        

def zacks_rank(Symbol):
 
    # Wait for 2 seconds
    #time.sleep(2)
    url = 'https://quote-feed.zacks.com/index?t='+Symbol
    downloaded_data  = urllib.request.urlopen(url)
    data = downloaded_data.read()
    data_str = data.decode()
    Z_Rank =["Strong Buy","Buy","Hold","Strong Sell", "Sell"]
    zack_dic = {'Strong Buy':1, 'Buy':2, 'Hold':3, 'Sell':4, 'Strong Sell':5}
    for Rank in Z_Rank:
       #data_str.find(Rank)# az tooye list Z_Rank doone doone check kon va yeki ra dar str_data
       # peyda kon ;; faghat index harf aval ro retrun mikond
       if(data_str.find(Rank) != -1):
           return zack_dic[Rank] #data_str[res:res+len(Rank)]#

parent_path = 'c:/Users/tsenh/github/little_high_mount_original/'

#ticker  = read_query(engine, 'select distinct ticker from awesome.hist_er')
ticker = pd.read_csv(parent_path + 'nasdaq_list/nasdaq09202024.csv', keep_default_na=False)

BAD_CHARS = ['.', '+', '-', '=', '$']
pat = '|'.join(['({})'.format(re.escape(c)) for c in BAD_CHARS])

ticker = ticker[~ticker['NASDAQ Symbol'].str.contains(pat)]

ticker_target = ticker.loc[(ticker['ETF'] == 'N')].reset_index(drop=True)


sector = []
industry = []
country = []
state = []
city = []
company_name = []


current_date = datetime.now().date()
tt = t.time()
for i in range(len(ticker_target)):
    t0 =  t.time()
    t_ticker = yf.Ticker(ticker_target['Symbol'][i])
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

    print('{0} takes {1} seconds'.format(ticker_target['Symbol'][i] , t.time()-t0))
    
print('All takes {0} seconds'.format(t.time()-tt))

ticker_target['sector'] = sector
ticker_target['industry'] = industry
ticker_target['country'] = country
ticker_target['state'] = state
ticker_target['city'] = city
ticker_target['company_name'] = company_name
ticker_target['last_update'] = [current_date]*len(ticker_target)

ticker1= ticker_target.replace(np.nan, '', regex=True)

#screen again to fill the blank

for i in range(len(ticker1)):
    tt0 = t.time()
    if ticker1['sector'][i] == '':    
        tt_ticker = yf.Ticker(ticker1['Symbol'][i])
        try:
            ticker1['sector'][i] = tt_ticker.info['sector']
            print('{0} refill sector info'.format(ticker1['Symbol'][i]))
        except:
            print('{0} still with no sector info'.format(ticker1['Symbol'][i]))
            
        if ticker1['industry'][i] == '':
           try:
               ticker1['industry'][i] = tt_ticker.info['industry']
               print('{0} refill industry info'.format(ticker1['Symbol'][i] ))
           except:
               print('{0} still with no industry info'.format(ticker1['Symbol'][i]))
               
        if ticker1['country'][i] == '':
           try:
               ticker1['country'][i] = tt_ticker.info['country']
               print('{0} refill country info'.format(ticker1['Symbol'][i] ))
           except:
               print('{0} still with no country info'.format(ticker1['Symbol'][i]))
               
        if ticker1['state'][i] == '':
           try:
               ticker1['state'][i] = tt_ticker.info['state']
               print('{0} refill state info'.format(ticker1['Symbol'][i] ))
           except:
               print('{0} still with no state info'.format(ticker1['Symbol'][i]))
               
        if ticker1['city'][i] == '':
           try:
               ticker1['city'][i] = tt_ticker.info['city']
               print('{0} refill city info'.format(ticker1['Symbol'][i] ))
           except:
               print('{0} still with no city info'.format(ticker1['Symbol'][i]))
               
        if ticker1['company_name'][i] == '':
           try:
               ticker1['company_name'][i] = tt_ticker.info['longName']
               print('{0} refill company_name info'.format(ticker1['Symbol'][i] ))
           except:
               print('{0} still with no company_name info'.format(ticker1['Symbol'][i]))
               
    print('{0} takes {1} seconds'.format(ticker_target['Symbol'][i] , t.time()-tt0))
    
print('All takes {0} seconds'.format(t.time()-tt))
        
final_ticker = ticker1.loc[ticker1['sector']!= '',].reset_index(drop=True)
final_ticker.to_sql(name='company_info', con=engine, schema = 'awesome', if_exists='replace', index = False)