# -*- coding: utf-8 -*-
"""
Created on Fri Dec 31 00:50:54 2021

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
        
        
def get_price_data(hist_earning):
    
    current_close_price = []
    nextday_close_price = []
    current_volume = []
    nextday_volume = []
    
    tt = t.time()
    for i in range(len(hist_earning)):
        t0 = t.time()
        symbol = hist_earning['ticker'][i]
        start_date = hist_earning['date'][i]
        end_date = start_date + relativedelta(days = 10)
        
        try:
            temp_price = web.DataReader(symbol, 'yahoo', start_date, end_date).iloc[0:2].reset_index()
            
            if len(temp_price)!=2:
                
                current_close_price.append(np.nan)
                current_volume.append(np.nan)
                nextday_close_price.append(np.nan)
                nextday_volume.append(np.nan)
            else:
        
                current_close_price.append(temp_price.iloc[0]['Close'])
                current_volume.append(temp_price.iloc[0]['Volume'])
                nextday_close_price.append(temp_price.iloc[1]['Close'])
                nextday_volume.append(temp_price.iloc[1]['Volume'])
        except:
            current_close_price.append(np.nan)
            current_volume.append(np.nan)
            nextday_close_price.append(np.nan)
            nextday_volume.append(np.nan)
            
        print('{0} takes {1} seconds'.format(symbol + ' ' + str(start_date), t.time()-t0))
        
    print('All takes {0} seconds'.format(t.time()-tt))
    
    price_info = pd.DataFrame(list(zip(current_close_price, nextday_close_price, current_volume, nextday_volume)), 
                          columns = ['current_close_price', 'nextday_close_price', 'current_volume', 'nextday_volume'])
    
    daydream = pd.concat([hist_earning, price_info], axis =1)
    

    return daydream


#any list of stock tickers make sense 
parent_path = 'c:/Users/tsenh/github/awesome/'   
initial_stock_list = pd.read_csv(parent_path + 'nasdaq.csv')
ticker_list = initial_stock_list

retry_list  = []

hist_er = pd.DataFrame()

for ticker in ticker_list['ticker']:
    
    t0 =t.time()
    try:
        
        temp_data = pd.DataFrame(si.get_earnings_history(ticker))
        temp_filter = temp_data.loc[(temp_data['startdatetime']>'2019-01-01') & (temp_data['startdatetimetype'] == 'TNS')]
        temp_final = temp_filter[['ticker', 'startdatetime', 'epsestimate', 'epsactual', 'epssurprisepct']]
        print('Grab earning report data for {0} , used {1} seconds.'.format(ticker,  str(t.time() - t0)))
    except:
        print('{0} cannot be captured'.format(ticker))
        retry_list.append(ticker)
        temp_final = pd.DataFrame()
    if len(temp_final)>0:
        
        hist_er = pd.concat([hist_er, temp_final])
        t.sleep(12)
#could run multiple time        
for ticker in retry_list:
    
    t0 =t.time()
    try:
        
        temp_data = pd.DataFrame(si.get_earnings_history(ticker))
        temp_filter = temp_data.loc[(temp_data['startdatetime']>'2019-01-01') & (temp_data['startdatetimetype'] == 'TNS')]
        temp_final = temp_filter[['ticker', 'startdatetime', 'epsestimate', 'epsactual', 'epssurprisepct']]
        print('Grab earning report data for {0} , used {1} seconds.'.format(ticker,  str(t.time() - t0)))
    except:
        print('{0} cannot be captured'.format(ticker))
        #retry_list.append(ticker)
        temp_final = pd.DataFrame()
    if len(temp_final)>0:
        
        hist_er = pd.concat([hist_er, temp_final])
        t.sleep(12)
        
hist_er_clean = hist_er.dropna()
hist_er_clean.insert(1, 'date', hist_er_clean['startdatetime'].str.split('T').str[0])
hist_er_clean['date'] = pd.to_datetime(hist_er_clean['date'])
hist_er_final = hist_er_clean[['ticker', 'date', 'epsestimate', 'epsactual','epssurprisepct']].sort_values(by = ['ticker', 'date']).reset_index(drop=True)


    
daydream = get_price_data(hist_er_final).drop_duplicates()

daydream = daydream.dropna()

daydream.to_sql(name='hist_er', con=engine, schema = 'awesome', if_exists='replace', index = False)