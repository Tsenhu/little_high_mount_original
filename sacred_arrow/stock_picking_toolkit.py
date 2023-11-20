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

#from urllib.parse import quote_plus as urlquote
#import yahoo_fin.stock_info as si
import yfinance as yf
#import urllib.request
import matplotlib.pyplot as plt
#from numpy import mean
import seaborn as sns


symbol_list = ['TSLA', 'DXCM', 'AMZN', 'PLTR', 'COIN', 'NFLX', 'ROKU']



def price_tracker(symbol_list:list):
    
    '''
    track 1 year's stock price change and find correlation between them
    '''
    
    end_date = dt.datetime.now().date()
    start_date = end_date - relativedelta(days = 365)
    df = pd.DataFrame()
    
    for i in range(len(symbol_list)):
        
        symbol = symbol_list[i]
        
        temp_price = yf.Ticker(symbol).history(start=start_date, end=end_date).reset_index()
        temp_price['ticker'] = symbol
        temp_price['price_change'] = (temp_price['Close']-temp_price['Open'])/temp_price['Open']
        
        df = pd.concat([df, temp_price], axis= 0)
        
    pivot_df = df.pivot(index= 'Date', columns = 'ticker', values= 'price_change')
    
    plt.figure(figsize=(8, 6))
    sns.heatmap(pivot_df.corr(), annot=True, cmap='coolwarm', linewidths=.5)
    plt.title('Correlation Heatmap')
    plt.show()
        
    return df, pivot_df
        
#test,_ = price_tracker(symbol_list)
        




def option_tracker(ticker='TSLA', expiration_date = "2023-12-15" ):
    '''
    track a ticker's option price for a certain date
    '''
    stock = yf.Ticker(ticker)
    
    # Get options data for the specified expiration date
    options = stock.option_chain(expiration_date)

    # Access call and put option data
    call_options = options.calls
    put_options = options.puts

    return call_options, put_options

_, p = option_tracker()