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


def simulate_series(num_steps):
    '''
    simulate accumulative yearly return for a long time with different probability and return rates
    '''
    series = []
    
    for _ in range(num_steps):
        # Generate a random number to determine whether it's a positive or negative return
        probability = random.uniform(0, 1)
        
        if probability <= 0.2:  # 20% probability for negative return
            return_value = random.uniform(-0.5, -0.2)
        else:  # 80% probability for positive return
            return_value = random.uniform(0.5, 1)
        
        series.append(round(return_value,4)+1)

    return reduce(lambda x,y:x*y, series)


temp = []
for i in range(10000):
    temp.append(simulate_series(30))
    
plt.hist(temp, bins='auto', alpha=0.7, color='blue', edgecolor='black')    
quantiles = np.percentile(temp, [25, 50, 75])


def transaction_tracker(ticker_list=['TSLA']):
    
    curr_date = dt.datetime.now().date() + relativedelta(days=1)
    initial_date = curr_date - relativedelta(days = 365*5)
    
    transaction_tracker = pd.DataFrame(columns = ['ticker', 'day0_volume_M', 'day0_amount_M', 'day1_volume_M', 'day1_amount_M', 
                                                  'week1_volume_M', 'week1_amount_M', 'week2_volume_M', 'week2_amount_M', 
                                                  'week4_volume_M', 'week4_amount_M', 'week12_volume_M', 'week12_amount_M',
                                                  'week26_volume_M', 'week26_amount_M', 'week52_volume_M', 'week52_amount_M',
                                                  'week156_volume_M', 'week156_amount_M'])
    for i, ticker in enumerate(ticker_list):
        
        temp_row = [ticker]
        temp_data = yf.Ticker(ticker).history(start=initial_date, end=curr_date).reset_index()
        
        temp_data['trans_amount'] = (temp_data['Open'] + temp_data['Close'])/2*temp_data['Volume']/1000000 # M level
        
        #0_day transaction volume and amount
        day0_volume_M = temp_data.iloc[-1]['Volume']/1000000 # M level
        day0_amount_M = temp_data.iloc[-1]['trans_amount'] # M level
        
        #1_day transaction volume and amount
        day1_volume_M = temp_data.iloc[-2]['Volume']/1000000 # M level
        day1_amount_M = temp_data.iloc[-2]['trans_amount']
        
        #1_week transaction volume and amount
        week1_volume_M = np.mean(temp_data.iloc[-6:-1]['Volume'])/1000000 # M level
        week1_amount_M = np.mean(temp_data.iloc[-6:-1]['trans_amount'])
        
        #2_week transaction volume and amount
        week2_volume_M = np.mean(temp_data.iloc[-5*2-1:-1]['Volume'])/1000000 # M level
        week2_amount_M = np.mean(temp_data.iloc[-5*2-1:-1]['trans_amount'])
        
        #4_week transaction volume and amount
        week4_volume_M = np.mean(temp_data.iloc[-5*4-1:-1]['Volume'])/1000000 # M level
        week4_amount_M = np.mean(temp_data.iloc[-5*4-1:-1]['trans_amount'])
        
        #12_week transaction volume and amount
        week12_volume_M = np.mean(temp_data.iloc[-5*12-1:-1]['Volume'])/1000000 # M level
        week12_amount_M = np.mean(temp_data.iloc[-5*12-1:-1]['trans_amount'])
        
        #26_week transaction volume and amount
        week26_volume_M = np.mean(temp_data.iloc[-5*26-1:-1]['Volume'])/1000000 # M level
        week26_amount_M = np.mean(temp_data.iloc[-5*26-1:-1]['trans_amount'])
        
        #52_week transaction volume and amount
        week52_volume_M = np.mean(temp_data.iloc[-5*52-1:-1]['Volume'])/1000000 # M level
        week52_amount_M = np.mean(temp_data.iloc[-5*52-1:-1]['trans_amount'])
        
        #156_week transaction volume and amount
        week156_volume_M = np.mean(temp_data.iloc[-5*156-1:-1]['Volume'])/1000000 # M level
        week156_amount_M = np.mean(temp_data.iloc[-5*156-1:-1]['trans_amount'])
        
        temp_row += [day0_volume_M, day0_amount_M, day1_volume_M, day1_amount_M, week1_volume_M, week1_amount_M,
                     week2_volume_M, week2_amount_M, week4_volume_M, week4_amount_M, week12_volume_M, week12_amount_M,
                     week26_volume_M, week26_amount_M, week52_volume_M, week52_amount_M, week156_volume_M, week156_amount_M]
        
        transaction_tracker.loc[len(transaction_tracker)] = temp_row
        
    return transaction_tracker

test = transaction_tracker(['TSLA', 'DXCM', 'QCOM', 'PDD'])

test_again = test.transpose()

'''

'''
def weekly_change(ticker:str, start_year = 2021):
    
    start_date = dt.datetime(start_year,1,1) - relativedelta(days=7)
    end_date = dt.datetime.now().date() + relativedelta(days=1)
    
    stock_info = yf.Ticker(ticker).history(start=start_date, end=end_date).reset_index()
    
    stock_info['price_change'] = round((stock_info['Close']-stock_info['Close'].shift(1))/stock_info['Close'].shift(1),4)
    
    #https://www.w3schools.com/python/python_datetime.asp
    
    stock_info['year_int'] = stock_info['Date'].apply(lambda x: int(x.strftime('%Y')))
    
    
    
    stock_info['month_str'] = stock_info['Date'].apply(lambda x: x.strftime('%b'))
    stock_info['month_int'] = stock_info['Date'].apply(lambda x: int(x.strftime('%m')))
    
    stock_info['weekday_str'] = stock_info['Date'].apply(lambda x: x.strftime('%a'))
    stock_info['weekday_int'] = stock_info['Date'].apply(lambda x: int(x.strftime('%w')))
    
    stock_radar = stock_info[stock_info['year_int']>2020]
    
    first_rows = stock_radar.groupby(['year_int','month_str']).head(1).reset_index()
    last_rows = stock_radar.groupby(['year_int','month_str']).tail(1).reset_index()
    
    month_change = pd.concat([first_rows, last_rows], axis=0).sort_values(by= 'Date')
    
    last_rows['month_diff'] = round((last_rows['Close'] - first_rows['Close'])/first_rows['Close'], 4)