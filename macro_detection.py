# -*- coding: utf-8 -*-
"""
Created on Sat Dec 10 19:10:07 2022

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
import yfinance as yf
import csv
import matplotlib.pyplot as plt

#from single_stock_analysis import plot_stock

t_ini = t.time()
_host = '127.0.0.1'
_db = 'awesome'
_user = 'root'
_password = 'Albert@25'
engine = create_engine('mysql://'+_user+':'+urlquote(_password)+'@'+_host)
save_path = 'C:/Users/tsenh/GitHub/extra_files/weekly_screen_' + str(datetime.now().date()) +'/'

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
        
        
text = '\
select update_date,\
sum(case when zack_rank = 1 then 1 else 0 end) as zack_1, \
sum(case when zack_rank = 2 then 1 else 0 end) as zack_2, \
sum(case when zack_rank = 3 then 1 else 0 end) as zack_3, \
sum(case when zack_rank = 4 then 1 else 0 end) as zack_4, \
sum(case when zack_rank = 5 then 1 else 0 end) as zack_5 \
from awesome.ticker_zack_hist \
group by update_date \
' 
df = read_query(engine, text)

df['pos_neg_total'] = df['zack_1'] + df['zack_2'] + df['zack_4'] + df['zack_5']
df['zack_1_pct'] = df['zack_1']/df['pos_neg_total']
df['zack_2_pct'] = df['zack_2']/df['pos_neg_total']
df['zack_4_pct'] = df['zack_4']/df['pos_neg_total']
df['zack_5_pct'] = df['zack_5']/df['pos_neg_total']
fig, ax1 = plt.subplots()
fig.set_size_inches(12, 6)
'''
ax2 = ax1.twinx()
ax3 = ax1.twinx()
ax4 = ax1.twinx()
ax5 = ax1.twinx()
'''

#ax3.spines.right.set_position(("axes", 1.1))
ln1 = ax1.plot(df['update_date'], df['zack_1_pct'], '-g',label = 'zack_1_pct')
ln2 = ax1.plot(df['update_date'], df['zack_2_pct'], color = 'c', linestyle = '-',  label = 'zack_2_pct')
#ln3 = ax3.plot(df['update_date'], df['zack_3'], color = 'y', linestyle = '-.', label = 'zack_3_pct')
ln4 = ax1.plot(df['update_date'], df['zack_4_pct'], color = 'm', linestyle = '-.', label = 'zack_4_pct')
ln5 = ax1.plot(df['update_date'], df['zack_5_pct'], color = 'r', linestyle = '-.', label = 'zack_5_pct')

lns = ln1+ln2+ln3+ln4+ln5
labs = [l.get_label() for l in lns]

ax1.legend(lns, labs, loc='upper left')
ax1.set_xlabel('updaet_date')
ax1.set(ylabel = 'avg_volume_million', title = ' hist Volume&Amount' )
#ax2.set_ylabel('avg_amount_billion')
#ax3.set_ylabel('avg_price')

plt.show()