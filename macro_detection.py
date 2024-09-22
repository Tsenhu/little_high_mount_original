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
from bs4 import BeautifulSoup
import requests
import time as t
from dateutil.relativedelta import relativedelta
import pandas_datareader.data as web
from urllib.parse import quote_plus as urlquote
import yahoo_fin.stock_info as si
import yfinance as yf
import csv
import matplotlib.pyplot as plt
from urllib.request import urlopen, Request

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
        
def zack_plot():        
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
    
def fundamental_metric(soup, metric):
    return soup.find(text = metric).find_next(class_='snapshot-td2').text

def get_fundamental_data(df):
    tt = t.time()
    for symbol in df.index:
        t0 = t.time()
        try:
            url = ("http://finviz.com/quote.ashx?t=" + symbol.lower())
            req = Request(url=url,headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}) 
            response = urlopen(req)
            soup = BeautifulSoup(response)
            for m in df.columns:                
                df.loc[symbol,m] = fundamental_metric(soup,m)                
        except Exception as e:
            print (symbol, 'not found')
        print('{0} takes {1} seconds for financial info'.format(symbol , t.time()-t0))
    print('All takes {0} seconds, average {1} seconds per ticker'.format(t.time()-tt, (t.time()-tt)/len(df)))
    return df



def comp_fa():
    metric = [
              'Market Cap',
              'P/B',
              'P/E',
              'Forward P/E',
              'PEG',
              'Debt/Eq',
              'EPS (ttm)',
              'ROE',
              'ROI',
              'EPS Q/Q',
              'Inst Own',
              'Perf YTD',
              'Prev Close',
              '52W High',
              '52W Low',
              '52W Range'
              ]
    
    #ticker_list = ['TSLA', 'DXCM', 'AMZN', 'GOOS']
    ticker_list  = read_query(engine, "select symbol as ticker from awesome.company_info")['ticker'].tolist()
    df = pd.DataFrame(index=ticker_list,columns=metric)
    df = get_fundamental_data(df)
    
    refine_df = df.loc[~df['P/B'].isnull() ].reset_index()
    refine_df.rename(columns = {'index':'ticker', 'P/B': 'PB', 'P/E': 'PE', 'Debt/Eq':'Debt_Eq'}, inplace=True)
    refine_df.to_sql(name='company_fa', con=engine, schema = 'awesome', if_exists='replace', index = False)
    
comp_fa()
comp_sql = 'SELECT fa.*, info.sector, info.industry, info.country \
            FROM awesome.company_fa fa \
            left join awesome.company_info info on fa.ticker = info.symbol \
            where fa.PB != "-" and fa.PE != "-"  \
            '
            
comp_info = read_query(engine, comp_sql)

comp_target  = comp_info.loc[(comp_info['Market Cap']!= '-') & (comp_info['Debt_Eq']!= '-'), ]

comp_target['mk_cap'] = comp_target['Market Cap'].apply(lambda x: float(x[:-1])*1000000 if x[-1] == 'M' else float(x[:-1])*1000000000)


comp_target['lowest'] = comp_target['52W Range'].apply(lambda x: float(x.split('-')[0].strip()) if x.split('-')[0].strip() else None)
comp_target['highest'] = comp_target['52W Range'].apply(lambda x: float(x.split('-')[1].strip()) if x.split('-')[1].strip() else None)

comp_target[['PE', 'PB', 'Forward P/E', 'Debt_Eq', 'EPS (ttm)']] = comp_target[['PE', 'PB', 'Forward P/E', 'Debt_Eq', 'EPS (ttm)']].apply(pd.to_numeric, errors='coerce')

comp_target.loc[comp_target['sector'] == 'Financial','sector'] = 'Financial Services'

comp_target['ROE'] = comp_target['ROE'].str.replace('%','')
comp_target['ROI'] = comp_target['ROI'].str.replace('%','')
comp_target['Inst Own'] = comp_target['Inst Own'].str.replace('%','')

comp_target[['PEG', 'ROE', 'ROI', 'Inst Own', 'Prev Close']] = comp_target[['PEG', 'ROE', 'ROI', 'Inst Own', 'Prev Close']].apply(pd.to_numeric, errors = 'coerce')

comp_target['ROE'] = comp_target['ROE']/100
comp_target['ROI'] = comp_target['ROI']/100
comp_target['Inst Own'] = comp_target['Inst Own']/100

comp_target['return_max'] = comp_target['highest']/comp_target['Prev Close']
comp_target['etl_date'] = dt.datetime.now().date()

test = comp_target.groupby(['sector'])[['PB', 'PE', 'ROE', 'ROI', 'Inst Own','return_max']].agg(['min', 'mean', 'median', 'max'])