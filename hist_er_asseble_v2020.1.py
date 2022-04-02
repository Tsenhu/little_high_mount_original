# -*- coding: utf-8 -*-
"""
Created on Sun Jan 12 16:19:50 2020

@author: tsenh
"""

'''
https://towardsdatascience.com/a-comprehensive-guide-to-downloading-stock-prices-in-python-2cd93ff821d4

alpha vantage api: DHXUWE4M05O9WHWW
                   NWZPVM5PMTELG7KX
                   9YAS0QPC5GFJ1PPP
'''
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

#parent_path = 'c:/Users/tsenh/github/awesome/'

    
last_db_date = read_query(engine, 'select max(date) as date from awesome.hist_er').iloc[0]['date']
#last_db_date = dt.date(2019,1,1)

def get_earning_data(date_from = (last_db_date + dt.timedelta(1)), date_to = (datetime.now().date()-dt.timedelta(5))):
    
    temp_hist_er = pd.DataFrame()
    
    t0 = t.time()
    
    temp_name = ['temp_'+ str(name) for name in range(12)]

    container = {}

    for i in range(len(temp_name)):
        
        container[temp_name[i]] = si.get_earnings_in_date_range(date_from, date_to)
        print('{0} times approach to get earning data'.format(str(i)))
        t.sleep(3)
            
    if len(container)>0:
        
        length_cnt = [len(container[name_key]) for name_key in container]
        
        temp_hist_er = pd.DataFrame(container['temp_'+ str(length_cnt.index(max(length_cnt)))])
    
        
        
        
        temp_hist_er.insert(1, 'date', temp_hist_er['startdatetime'].str.split('T').str[0])
        temp_hist_er['date'] = pd.to_datetime(temp_hist_er['date'])
        temp_hist_er = temp_hist_er[['ticker', 'date', 'epsestimate', 'epsactual','epssurprisepct']].sort_values(by = ['ticker', 'date']).reset_index(drop=True)
        
        temp_hist_er = temp_hist_er.dropna()
    print('Grab earning report data from {0} to {1}, used {2} seconds.'.format(str(date_from), str(date_to),  str(t.time() - t0)))
    
    return temp_hist_er

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


def get_price_data(hist_earning):
    
    current_close_price = []
    nextday_close_price = []
    current_volume = []
    nextday_volume = []
    zack_rank = []
    
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
         
        print('Get price of {0} takes {1} seconds'.format(symbol + ' ' + str(start_date), t.time()-t0))
        
    print('Get price of all takes {0} seconds'.format(t.time()-tt))
    
    price_info = pd.DataFrame(list(zip(current_close_price, nextday_close_price, current_volume, nextday_volume)), 
                          columns = ['current_close_price', 'nextday_close_price', 'current_volume', 'nextday_volume'])
    
    daydream = pd.concat([hist_earning, price_info], axis =1)
    

    return daydream



#different version compare to hist_er_base_initial.py
'''
def recommendation_cnt(ticker_list, hist_er):
    daydream_final = pd.DataFrame()
    tt = t.time()
    for ticker in ticker_list:
        t0 = t.time()
        
        temp_daydream = hist_er.loc[hist_er['ticker'] == ticker].reset_index()
        temp_strong_buy = []
        temp_buy = []
        temp_hold = []
        temp_sell = []
        temp_strong_sell = []
        temp_total_recommendations = []
        try:
            temp_recommendations = yf.Ticker(ticker).recommendations.reset_index()
        
        except:
            temp_recommendations = pd.DataFrame()
            
        if len(temp_recommendations)>0:
            
            temp_start_date = read_query(engine, "select date from awesome.hist_er where ticker = '" + ticker +"' order by date desc limit 1")['date']
            
            if len(temp_start_date):
                
                temp_start_date = temp_start_date[0]
            
            else:
                
                temp_start_date = dt.datetime(2018,1,10)
            
                    
            temp_end_date = datetime.combine(temp_daydream['date'][0].date(), datetime.min.time())
            
            sliced_rec = temp_recommendations.loc[(temp_recommendations['Date']>= temp_start_date) &  (temp_recommendations['Date']<= temp_end_date)]
            
            temp_strong_buy.append(len(sliced_rec.loc[sliced_rec['To Grade'] == 'Strong Buy']))
            temp_buy.append(len(sliced_rec.loc[sliced_rec['To Grade'] == 'Buy']))
            temp_hold.append(len(sliced_rec.loc[sliced_rec['To Grade'] == 'Hold']))
            temp_sell.append(len(sliced_rec.loc[sliced_rec['To Grade'] == 'Sell']))
            temp_strong_sell.append(len(sliced_rec.loc[sliced_rec['To Grade'] == 'Strong Sell']))
            temp_total_recommendations.append(len(sliced_rec))
            
            print('Finish recommendation count for {0} , used {1} seconds.'.format(ticker,  str(t.time() - t0)))
            t.sleep(5)
        else:
            for i in range(len(temp_daydream)):
                temp_strong_buy.append(np.nan)
                temp_buy.append(np.nan)
                temp_hold.append(np.nan)
                temp_sell.append(np.nan)
                temp_strong_sell.append(np.nan)
                temp_total_recommendations.append(np.nan)
                
            print('Cannot match {0}'.format(ticker))
            
        recommendation_info = pd.DataFrame(list(zip(temp_strong_buy, temp_buy, temp_hold, temp_sell, temp_strong_sell, temp_total_recommendations)), 
                                  columns = ['strong_buy_cnt', 'buy_cnt', 'hold_cnt', 'sell_cnt', 'strong_sell_cnt', 'total_recommendations'])
            
        temp_daydream_final = pd.concat([temp_daydream, recommendation_info], axis =1)
        
        daydream_final = pd.concat([daydream_final, temp_daydream_final], axis = 0)
        
        
    print('All takes {0} seconds'.format(t.time()-tt))
    return daydream_final
'''
#grab new er stock base info for the certain range
if ((datetime.now().date()-dt.timedelta(5)) - (last_db_date + dt.timedelta(1)).date()).days >= 0:
    hist_earning = get_earning_data().drop_duplicates().reset_index(drop=True)
else:
    hist_earning = pd.DataFrame()

if len(hist_earning)>0:
    
    daydream = get_price_data(hist_earning)
    
    daydream['etl_date'] = pd.to_datetime(datetime.now().date())
    
    daydream_final = daydream[['ticker', 'date', 'epsestimate', 'epsactual', 'epssurprisepct',
           'current_close_price', 'nextday_close_price', 'current_volume',
           'nextday_volume', 'etl_date']]

    daydream_final.to_sql(name='hist_er', con=engine, schema = 'awesome', if_exists='append', index = False)

#prepare for elite table
new_daydream = read_query(engine, 'select * from awesome.hist_er')

new_daydream['price_change'] = new_daydream['nextday_close_price']/new_daydream['current_close_price'] - 1


volume_detect = new_daydream.groupby('ticker')['current_volume', 'nextday_volume'].mean().reset_index()

volume_detect_elite = volume_detect[(volume_detect['current_volume'] + volume_detect['nextday_volume'])/2 >= 1000000]

daydream_elite = new_daydream[new_daydream['ticker'].isin(volume_detect_elite['ticker'].to_list())]
daydream_elite.to_sql(name='hist_er_elite', con=engine, schema = 'awesome', if_exists='replace', index = False)





