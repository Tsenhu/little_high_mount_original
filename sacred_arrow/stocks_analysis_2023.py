# -*- coding: utf-8 -*-
"""
Created on Fri Dec 29 19:14:44 2023

@author: tsenh
"""
import pandas as pd
#from sqlalchemy import create_engine
#import sqlalchemy
import datetime as dt
from datetime import datetime, timedelta
#import talib as ta
#import numpy as np
#import os, os.path 
#from collections import OrderedDict
#import re
#import bs4
#import requests
import time as t
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
#from urllib.parse import quote_plus as urlquote
#import yahoo_fin.stock_info as si
import yfinance as yf
#import urllib.request
import matplotlib.pyplot as plt
#from numpy import mean
import seaborn as sns
import random
import numpy as np

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
        

''' TO DO
0. rerun company_fa
1. load company_fa data to get fa info and ticker list
stock_list =  read_query(engine, "select * from awesome.stock_er_weekly_update where er_date<curdate()")
2. add time record to the code

'''
def price_tracker(symbol_list:list):
    
    '''
    track 1 year's stock price change and find correlation between them
    '''
    
    end_date = dt.datetime.now().date() + relativedelta(days = 1)
    start_date = dt.datetime(2023,1,1)
    
    
    for i in range(len(symbol_list)):
        
        df = pd.DataFrame()
        symbol = symbol_list[i]
        
        ticker = []
        year_begin_price = []
        year_min_price = []
        year_max_price = []
        year_end_price = []
        avg_volume = []
        avg_amount = []
        price_change_min = []
        price_change_mean = []
        price_change_max = []
        
        
        temp_price = yf.Ticker(symbol).history(start=start_date, end=end_date).reset_index()
        temp_price['ticker'] = symbol
        temp_price['price_change'] = (temp_price['Close']-temp_price['Open'])/temp_price['Open']
        temp_price['amount'] = (temp_price['Low']+temp_price['High'])*temp_price['Volume']/2
        df = pd.concat([df, temp_price], axis= 0)
        
        ticker.append(symbol)
        year_begin_price.append(df['Close'][0])
        year_min_price.append(df['Close'].min())
        year_max_price.append(df['Close'].max())
        year_end_price.append(df['Close'].iloc[-1])
        avg_volume.append(df['Volume'].mean())
        avg_amount.append(df['amount'].mean())
        price_change_min.append(df['price_change'].min())
        price_change_mean.append(df['price_change'].mean())
        price_change_max.append(df['price_change'].max())
        
    price_info = pd.DataFrame(list(zip(ticker, year_begin_price, year_min_price, year_max_price, year_end_price,
                                       avg_volume, avg_amount,
                                       price_change_min, price_change_mean, price_change_max)), 
                              columns = ['ticker', 'year_begin_price', 'year_min_price', 'year_max_price', 'year_end_price',
                                         'avg_volume', 'avg_amount',
                                         'price_change_min', 'price_change_mean', 'price_change_max'])

    return price_info