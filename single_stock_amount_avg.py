# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 20:26:18 2022

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
import yfinance as yf
import urllib.request
import matplotlib.pyplot as plt
from numpy import mean

end_date = dt.datetime.now().date()
start_date = end_date - relativedelta(days = 365)
#symbol = 'DXCM'

def get_price_data(symbol):
    
    ticker = []
    date_category = []
    avg_volume_million = []
    avg_amount_billion = []
    avg_price = []
    
    date_category_list = ['1D', '1W', '2W', '1M', '3M', '6M']
    
    tt = t.time()
    
    t0 = t.time()
    
    end_date = dt.datetime.now().date()
    start_date = end_date - relativedelta(days = 365)
    
    try:
        temp_price = web.DataReader(symbol, 'yahoo', start_date, end_date)
        temp_price['avg_price'] = temp_price.apply(lambda x: (x['High'] + x['Low'] + x['Open'] + x['Close'])/4, axis=1)
        temp_price['amount'] = temp_price.apply(lambda x: x['avg_price']*x['Volume'], axis =1)
        
        
        for dc in date_category_list:
            
            ticker.append(symbol)
            
            if dc == '1D':
                date_category.append(str(end_date))
                avg_volume_million.append(round(temp_price['Volume'][-1]/1000000,2))
                avg_amount_billion.append(round(temp_price['amount'][-1]/100000000,2))
                avg_price.append(round(mean(temp_price['Close'][-1]),2))
            if dc == '1W':
                date_category.append(dc)
                avg_volume_million.append(round(sum(temp_price['Volume'][-5:])/5/1000000,2))
                avg_amount_billion.append(round(sum(temp_price['amount'][-5:])/5/100000000,2))
                avg_price.append(round(mean(temp_price['Close'][-5:]),2))
            if dc == '2W':
                date_category.append(dc)
                avg_volume_million.append(round(sum(temp_price['Volume'][-10:])/10/1000000,2))
                avg_amount_billion.append(round(sum(temp_price['amount'][-10:])/10/100000000,2))
                avg_price.append(round(mean(temp_price['Close'][-10:]),2))
            if dc == '1M':
                date_category.append(dc)
                avg_volume_million.append(round(sum(temp_price['Volume'][-30:])/30/1000000,2))
                avg_amount_billion.append(round(sum(temp_price['amount'][-30:])/30/100000000,2))    
                avg_price.append(round(mean(temp_price['Close'][-30:]),2))
            if dc == '3M':
                date_category.append(dc)
                avg_volume_million.append(round(sum(temp_price['Volume'][-90:])/90/1000000,2))
                avg_amount_billion.append(round(sum(temp_price['amount'][-90:])/90/100000000,2))   
                avg_price.append(round(mean(temp_price['Close'][-60:]),2))
            if dc == '6M':
                date_category.append(dc)
                avg_volume_million.append(round(sum(temp_price['Volume'][-180:])/180/1000000,2))
                avg_amount_billion.append(round(sum(temp_price['amount'][-180:])/180/100000000,2))
                avg_price.append(round(mean(temp_price['Close'][-180:]),2))
        
        yahoo_df = pd.DataFrame({'Symbol':symbol, 'date_category':date_category,
                                 'avg_volume_million':avg_volume_million, 
                                 'avg_amount_billion':avg_amount_billion,
                                 'avg_price':avg_price})
                
        df = yahoo_df
        
        fig, ax1 = plt.subplots()
        fig.set_size_inches(8, 4)
        ax2 = ax1.twinx()
        ax3 = ax1.twinx()
        ax3.spines.right.set_position(("axes", 1.1))
        ln1 = ax1.plot(df['date_category'], df['avg_volume_million'], '-g',label = 'avg_volume_million')
        ln2 = ax2.plot(df['date_category'], df['avg_amount_billion'], color = 'y', linestyle = '--',  label = 'avg_amount_billion')
        ln3 = ax3.plot(df['date_category'], df['avg_price'], color = 'm', linestyle = '-.', label = 'avg_price')

        lns = ln1+ln2+ln3
        labs = [l.get_label() for l in lns]

        ax1.legend(lns, labs, loc='upper left')
        ax1.set_xlabel('date')
        ax1.set(ylabel = 'avg_volume_million', title = symbol + ' hist Volume&Amount' )
        ax2.set_ylabel('avg_amount_billion')
        ax3.set_ylabel('avg_price')
        
        plt.show()
    
    except:

        return

    return yahoo_df

#df = get_price_data(symbol)

