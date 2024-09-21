# -*- coding: utf-8 -*-
"""
Created on Fri Dec 31 00:50:54 2021

@author: tsenh
"""

'''
1. load initial tickers from excel
2. use pandas_datareader to load ER dates stock price and volume
3. use yfinance to add insititution recommendations
'''

'''
alpha vantage api: DHXUWE4M05O9WHWW
                   NWZPVM5PMTELG7KX
                   9YAS0QPC5GFJ1PPP
                   1UFEXZQK35XSLQX8
                   J0MON5KYLM5IX3TU
                   GPWR823PM8D1HY0M
                   8Z4ES4L24CKRLIR3
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
        
def zacks_rank(Symbol):
 
    # Wait for 2 seconds
    #time.sleep(2)
    url = 'https://quote-feed.zacks.com/index?t='+Symbol
    downloaded_data  = urllib.request.urlopen(url,timeout=120)
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
    
    tt = t.time()
    for i in range(len(hist_earning)):
        t0 = t.time()
        symbol = hist_earning['ticker'][i]
        start_date = hist_earning['date'][i]
        end_date = start_date + relativedelta(days = 10)
        
        try:
            temp_price = yf.Ticker(symbol).history(start=start_date, end=end_date).iloc[0:2].reset_index()
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
#initial_stock_list = pd.read_csv(parent_path + 'nasdaq.csv')
ticker_list  = read_query(engine, "select symbol as ticker from awesome.company_info")
#ticker_list = initial_stock_list

#if we cannot catch the data in the first time we try it again later
retry_list = []
hist_er = pd.DataFrame()

for ticker in ticker_list['ticker']:
    
    t0 =t.time()
    
   
    try:
        #temp_data = pd.DataFrame(si.get_earnings_history(ticker))
        temp_ticker = yf.Ticker(ticker)
        temp_data = temp_ticker.get_earnings_dates(limit=20).reset_index().rename(columns ={'Earnings Date':'date', 
                                                                                             'EPS Estimate':'epsestimate', 
                                                                                             'Reported EPS':'epsactual', 
                                                                                             'Surprise(%)':'epssurprisepct'}).dropna()
        temp_data['ticker'] = ticker
        #temp_filter = temp_data.loc[(temp_data['date']>'2019-01-01')]
        temp_final = temp_data[['ticker', 'date', 'epsestimate', 'epsactual', 'epssurprisepct']]
        print('Grab earning report data for {0} , used {1} seconds.'.format(ticker,  str(t.time() - t0)))
    except:
        print('{0} cannot be captured'.format(ticker))
        retry_list.append(ticker)
        temp_final = pd.DataFrame()
    if len(temp_final)>0:
        
        hist_er = pd.concat([hist_er, temp_final])
        t.sleep(5)
#could run multiple time        
for ticker in retry_list:
    
    t0 =t.time()
    try:
        
        temp_ticker = yf.Ticker(ticker)
        temp_data = temp_ticker.get_earnings_dates(limit=40).reset_index().rename(columns ={'Earnings Date':'date', 
                                                                                            'EPS Estimate':'epsestimate', 
                                                                                            'Reported EPS':'epsactual', 
                                                                                            'Surprise(%)':'epssurprisepct'}).dropna()
        temp_data['ticker'] = ticker
        #temp_filter = temp_data.loc[(temp_data['date']>'2019-01-01')]
        temp_final = temp_data[['ticker', 'date', 'epsestimate', 'epsactual', 'epssurprisepct']]
        print('Grab earning report data for {0} , used {1} seconds.'.format(ticker,  str(t.time() - t0)))
    except:
        print('{0} cannot be captured'.format(ticker))
        #retry_list.append(ticker)
        temp_final = pd.DataFrame()
    if len(temp_final)>0:
        
        hist_er = pd.concat([hist_er, temp_final])
        t.sleep(12)
'''




for ticker in ticker_list['ticker']:
    t0= t.time()
    temp_url = 'https://www.alphavantage.co/query?function=EARNINGS&symbol=' + ticker + '&apikey=' + 'DHXUWE4M05O9WHWW'
    
    r = requests.get(temp_url)
    try:
        data = pd.DataFrame(r.json()['quarterlyEarnings'][:24])
    except:
        data = pd.DataFrame()
        retry_list.append(ticker)
        print('{0} cannot be found'.format(ticker))
        
    if len(data)>0:
        
        data['ticker'] = ticker
    
        hist_er = pd.concat([hist_er, data])
        print('{0} has been captured, cost {1} seconds'.format(ticker, str(t.time() - t0)))
    else:
        print('{0} has no data'.format(ticker))
    t.sleep(12)        

for ticker in retry_list:
    
    t0=t.time()
    temp_url = 'https://www.alphavantage.co/query?function=EARNINGS&symbol=' + ticker + '&apikey=' + 'DHXUWE4M05O9WHWW'
    
    r = requests.get(temp_url)
    try:
        data = pd.DataFrame(r.json()['quarterlyEarnings'][:24])
    except:
        data = pd.DataFrame()
        
        print('Second time: {0} cannot be found'.format(ticker))
        
    if len(data)>0:
        
        data['ticker'] = ticker
    
        hist_er = pd.concat([hist_er, data])
        print('Second time: {0} has been captured, cost {1} seconds'.format(ticker, str(t.time() - t0)))
    else:
        print('Second time: {0} has no data'.format(ticker))
    t.sleep(12)        

hist_er[['fiscalDateEnding', 'reportedDate']] = hist_er[['fiscalDateEnding', 'reportedDate']].apply(pd.to_datetime, errors='coerce')
hist_er[['reportedEPS', 'estimatedEPS', 'surprise', 'surprisePercentage']] = hist_er[['reportedEPS', 'estimatedEPS', 'surprise', 'surprisePercentage']].apply(pd.to_numeric, errors='coerce')
hist_er = hist_er.rename(columns = {'reportedDate':'date', 'reportedEPS':'epsactual', 'estimatedEPS':'epsestimate', 'surprisePercentage':'epssurprisepct'})
hist_er_clean = hist_er[['ticker', 'fiscalDateEnding', 'date', 'epsestimate', 'epsactual','epssurprisepct']].sort_values(by=['ticker', 'date']).reset_index(drop=True)
hist_er_final = hist_er_clean.dropna()
'''

hist_er['date'] = pd.to_datetime(hist_er['date'], utc=True).dt.date
hist_er_final = hist_er[['ticker', 'date', 'epsestimate', 'epsactual','epssurprisepct']].sort_values(by = ['ticker', 'date']).reset_index(drop=True)


   

daydream = get_price_data(hist_er_final).drop_duplicates()

daydream = daydream.dropna()

#recommendation cnt
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
            for i in range(len(temp_daydream)):
                if i == 0:
                    temp_start_date = dt.datetime(2018,1,10)
                else:
                    temp_start_date = temp_daydream['date'][i-1]
                    
                temp_end_date = temp_daydream['date'][i]
                
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

#daydream_final = recommendation_cnt(daydream_ticker, daydream)
daydream_final = daydream.copy()

#daydream_final = pd.merge(daydream_final, ticker_list, on='ticker')

daydream_final['etl_date'] = pd.to_datetime(datetime.now().date())

if 'index' in daydream_final.columns:
    daydream_final = daydream_final.drop(columns=['index'])
    

    
daydream_final.to_sql(name='hist_er', con=engine, schema = 'awesome', if_exists='replace', index = False)

text = 'SELECT * FROM awesome.hist_er \
where ticker in ( \
select ticker from ( \
select ticker, count(*) from awesome.hist_er group by ticker having count(*)>=7) a \
) '

new_daydream = read_query(engine, text)

new_daydream['price_change'] = new_daydream['nextday_close_price']/new_daydream['current_close_price'] - 1


volume_detect = new_daydream.groupby('ticker')[['current_volume', 'nextday_volume']].mean().reset_index()

volume_detect_elite = volume_detect[(volume_detect['current_volume'] + volume_detect['nextday_volume'])/2 >= 1000000]

daydream_elite = new_daydream[new_daydream['ticker'].isin(volume_detect_elite['ticker'].to_list())]
daydream_elite.to_sql(name='hist_er_elite', con=engine, schema = 'awesome', if_exists='replace', index = False)