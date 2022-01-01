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

'''
UPDATE portfolio for option
'''
#temp_option_rd = pd.read_excel(parent_path + 'option_rd.xlsx')

#temp_option_rd.to_sql(name='option_portfolio', con=engine, schema = 'awesome', if_exists='replace', index = False)



for i in range(12):
    
    date_from = dt.datetime(2019,1+i,1)
    date_to = date_from + relativedelta(months=1) - dt.timedelta(1)
    t0 = t.time()
    temp_aa = pd.DataFrame(yec.earnings_between(date_from, date_to))
    hist_er = pd.concat([hist_er,temp_aa])
    print(str(date_from) + '  data used ' + str(t.time() - t0) + ' seconds')
    t.sleep(10)
    
    

hist_earning =  hist_er[['ticker','startdatetime', 'epsestimate','epsactual','epssurprisepct']].dropna().sort_values(by = ['ticker', 'startdatetime']).reset_index()

last_db_date = read_query(engine, 'select max(date) as date from awesome.hist_er').iloc[0]['date']
#last_db_date = dt.date(2019,1,1)

def get_earning_data(date_from = (last_db_date + dt.timedelta(1)), date_to = (datetime.now().date()-dt.timedelta(5))):
    
    hist_er = pd.DataFrame()
    
    t0 = t.time()
    temp_date_from = date_from
    
    for i in range((date_to - date_from).days + 1):
        
        temp_date_from = date_from + dt.timedelta(i)
        print(temp_date_from)
        try:
            temp_hist_er = pd.DataFrame(yec.earnings_between(temp_date_from, temp_date_from))
            print(1)           
        except:
            try:
                temp_hist_er = pd.DataFrame(yec.earnings_between(temp_date_from, temp_date_from))
                print(2)
            except:
                try:
                    temp_hist_er = pd.DataFrame(yec.earnings_between(temp_date_from, temp_date_from))
                    print(3)
                except:
                    temp_hist_er = pd.DataFrame()
                    print(4)
        if len(temp_hist_er) > 0:
            
            temp_hist_er['date'] = temp_date_from
            
            hist_er = pd.concat([hist_er, temp_hist_er])
            
        
    print('Grab earning report data from {0} to {1}, used {2} seconds.'.format(str(date_from), date_to,  str(t.time() - t0)))
    
    return hist_er

if ((datetime.now().date()-dt.timedelta(5)) - (last_db_date + dt.timedelta(1))).days >= 0:
    hist_earning = get_earning_data()[['ticker','date', 'epsestimate','epsactual','epssurprisepct']].dropna().sort_values(by = ['ticker', 'date']).reset_index()




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
    
    daydream = pd.concat([hist_earning, price_info], axis =1).drop(columns = ['index'])
    

    return daydream
    
daydream = get_price_data(hist_earning).drop_duplicates()

daydream.to_sql(name='hist_er', con=engine, schema = 'awesome', if_exists='append', index = False)

#prepare for elite table
new_daydream = read_query(engine, 'select * from awesome.hist_er')

new_daydream['price_change'] = new_daydream['nextday_close_price']/new_daydream['current_close_price'] - 1


volume_detect = new_daydream.groupby('ticker')['current_volume', 'nextday_volume'].mean().reset_index()

volume_detect_elite = volume_detect[(volume_detect['current_volume'] + volume_detect['nextday_volume'])/2 >= 1000000]

daydream_elite = new_daydream[new_daydream['ticker'].isin(volume_detect_elite['ticker'].to_list())]
daydream_elite.to_sql(name='hist_er_elite', con=engine, schema = 'awesome', if_exists='replace', index = False)


'''
next earning date
'''
elite_ticker  = read_query(engine, 'select distinct ticker from awesome.hist_er_elite')

date = []

tt = t.time()
for i in range(len(elite_ticker)):
    t0 =  t.time()
    try:
        date.append(dt.datetime.utcfromtimestamp(yec.get_next_earnings_date(elite_ticker['ticker'][i])).date())

    except:
        date.append(np.nan)

    print('{0} takes {1} seconds'.format(elite_ticker['ticker'][i] , t.time()-t0))
print('All takes {0} seconds'.format(t.time()-tt))

elite_ticker['er_date'] = date

temp_elite_ticker = elite_ticker[elite_ticker['er_date']> datetime.now().date()]
temp_elite_ticker.to_sql(name='next_er_date', con=engine, schema = 'awesome', if_exists='replace', index = False)