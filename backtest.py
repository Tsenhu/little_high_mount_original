# -*- coding: utf-8 -*-
"""
Created on Fri Jan 28 20:31:35 2022

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
        

elite_ticker_list = read_query(engine, "select distinct ticker from awesome.hist_er_elite where date> '2021/1/1'")['ticker'].tolist()

elite_df =  read_query(engine, "select * from awesome.hist_er_elite where date> '2021/1/1'")

company_info = read_query(engine, 
                          "select symbol, sector from awesome.company_info where symbol in ('" + "','".join(elite_ticker_list) + "')")



w1_ahead = []
w2_ahead = []
w3_ahead = []
w4_ahead = []
w5_ahead = []
w6_ahead = []
w7_ahead = []
w8_ahead = []

for i in range(len(elite_df)):
    t0 =  t.time()
    symbol = elite_df['ticker'][i]
    start_date = elite_df['date'][i] - timedelta(days = 90)
    end_date = elite_df['date'][i] - timedelta(days=1)
    try:
        hist_price = web.DataReader(symbol, 'yahoo', start_date, end_date).reset_index()
    except:
        hist_price = pd.DataFrame()

    #1 week before
    if len(hist_price)>5:
        temp_price = hist_price.iloc[len(hist_price)-5:]
        w1_ahead.append(round((temp_price['High']+temp_price['Low']).mean()/2,2))
    else:
        w1_ahead.append(np.nan)
        
    #2 week before   
    if len(hist_price)>10:
        temp_price = hist_price.iloc[len(hist_price)-10:len(hist_price)-5]
        w2_ahead.append(round((temp_price['High']+temp_price['Low']).mean()/2,2))
    else:
        w2_ahead.append(np.nan)
       
    #3 week before    
    if len(hist_price)>15:
        temp_price = hist_price.iloc[len(hist_price)-15:len(hist_price)-10]
        w3_ahead.append(round((temp_price['High']+temp_price['Low']).mean()/2,2))
    else:
        w3_ahead.append(np.nan)
        
    #4 week before
    if len(hist_price)>20:
        temp_price = hist_price.iloc[len(hist_price)-20:len(hist_price)-15]
        w4_ahead.append(round((temp_price['High']+temp_price['Low']).mean()/2,2))
    else:
        w4_ahead.append(np.nan)
     
    #5 week before    
    if len(hist_price)>25:
        temp_price = hist_price.iloc[len(hist_price)-25:len(hist_price)-20]
        w5_ahead.append(round((temp_price['High']+temp_price['Low']).mean()/2,2))
    else:
        w5_ahead.append(np.nan)
        
    #6 week before
    if len(hist_price)>30:
        temp_price = hist_price.iloc[len(hist_price)-30:len(hist_price)-25]
        w6_ahead.append(round((temp_price['High']+temp_price['Low']).mean()/2,2))
    else:
        w6_ahead.append(np.nan)
        
    #7 week before
    if len(hist_price)>35:
        temp_price = hist_price.iloc[len(hist_price)-35:len(hist_price)-30]
        w7_ahead.append(round((temp_price['High']+temp_price['Low']).mean()/2,2))
    else:
        w7_ahead.append(np.nan)
        
    #8 week before
    if len(hist_price)>40:
        temp_price = hist_price.iloc[len(hist_price)-40:len(hist_price)-35]
        w8_ahead.append(round((temp_price['High']+temp_price['Low']).mean()/2,2))
    else:
        w8_ahead.append(np.nan)
    
    print('{0} {1} takes {2} seconds'.format(symbol, str(elite_df['date'][i]) , t.time()-t0))
    
elite_df['w8_ahead'] = w8_ahead

elite_df['w7_ahead'] = w7_ahead

elite_df['w6_ahead'] = w6_ahead

elite_df['w5_ahead'] = w5_ahead

elite_df['w4_ahead'] = w4_ahead

elite_df['w3_ahead'] = w3_ahead

elite_df['w2_ahead'] = w2_ahead

elite_df['w1_ahead'] = w1_ahead

#elite_df = pd.read_csv('C:/Users/tsenh/GitHub/little_high_mount_original/backtest_price.csv')

#feature engineering
backtest = elite_df.copy()

backtest['re_w8_ahead'] = round((backtest['nextday_close_price'] - backtest['w8_ahead'])/backtest['nextday_close_price'],3)
backtest['re_w7_ahead'] = round((backtest['nextday_close_price'] - backtest['w7_ahead'])/backtest['nextday_close_price'],3)
backtest['re_w6_ahead'] = round((backtest['nextday_close_price'] - backtest['w6_ahead'])/backtest['nextday_close_price'],3)
backtest['re_w5_ahead'] = round((backtest['nextday_close_price'] - backtest['w5_ahead'])/backtest['nextday_close_price'],3)
backtest['re_w4_ahead'] = round((backtest['nextday_close_price'] - backtest['w4_ahead'])/backtest['nextday_close_price'],3)
backtest['re_w3_ahead'] = round((backtest['nextday_close_price'] - backtest['w3_ahead'])/backtest['nextday_close_price'],3)
backtest['re_w2_ahead'] = round((backtest['nextday_close_price'] - backtest['w2_ahead'])/backtest['nextday_close_price'],3)
backtest['re_w1_ahead'] = round((backtest['nextday_close_price'] - backtest['w1_ahead'])/backtest['nextday_close_price'],3)

backtest_clean = backtest[['ticker', 'date', 'zack_rank', 'nextday_close_price', 're_w8_ahead', 're_w7_ahead', 're_w6_ahead', 're_w5_ahead',
're_w4_ahead', 're_w3_ahead', 're_w2_ahead', 're_w1_ahead']]

backtest_clean = pd.merge(backtest_clean, company_info, how = 'inner', left_on='ticker', right_on = 'symbol')

q_list = []
for ticker in elite_ticker_list:
    
    temp_backtest = backtest_clean.loc[backtest_clean['ticker'] == ticker]
    
    temp_q_list = []
    for i in range(len(temp_backtest)):
        if temp_q_list and temp_q_list[-1] != 'Q4':
            temp_q_list.append('Q'+str(int(temp_q_list[-1][1])+1))
        elif temp_q_list:
            temp_q_list.append('Q1')
            
        else:
            temp_q_list.append('Q4')
    
    
    q_list +=temp_q_list
    
backtest_clean['q_info'] = q_list
    
    
    
    
#analysis for zack rank return
return_zack_rank = backtest_clean.groupby(['zack_rank']).mean()
#analysis for quarter return
return_quarter = backtest_clean.groupby(['q_info']).mean()

return_quarter_zack1 = backtest_clean.loc[backtest_clean['zack_rank'] == 1].groupby(['q_info']).mean()

return_quarter_zack2 = backtest_clean.loc[backtest_clean['zack_rank'] == 2].groupby(['q_info']).mean()

return_quarter_zack12 = backtest_clean.loc[backtest_clean['zack_rank'] <=2].groupby(['q_info']).mean()
#analyssi for sector return
return_sector = backtest_clean.groupby(['sector']).mean()

return_sector_zack1 = backtest_clean.loc[backtest_clean['zack_rank'] == 1].groupby(['sector']).mean()

return_sector_zack2 = backtest_clean.loc[backtest_clean['zack_rank'] == 2].groupby(['sector']).mean()

return_sector_zack12 = backtest_clean.loc[backtest_clean['zack_rank'] <=2].groupby(['sector']).mean()
    
    
    
    
    
    
    
    
    
    