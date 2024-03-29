# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 21:12:02 2022

@author: tsenh
"""
#import talib as ta
from collections import OrderedDict
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import bs4
import datetime as dt
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import os, os.path 
import pandas as pd
import pandas_datareader.data as web
import re
import requests
import scipy.stats as stats
import sqlalchemy
import time as t
import urllib.request
import yahoo_fin.stock_info as si
import yfinance as yf
import matplotlib.ticker as ticker
import pathlib

t_ini = t.time()
_host = '127.0.0.1'
_db = 'awesome'
_user = 'root'
_password = 'Albert@25'
engine = create_engine('mysql://'+_user+':'+urlquote(_password)+'@'+_host)
save_path = 'C:/Users/tsenh/GitHub/extra_files/weekly_screen_' + str(datetime.now().date()) +'/'

path = pathlib.Path(save_path)
path.mkdir(parents=True, exist_ok = True)

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


def get_stock_data(ticker, start_year):
    
    return  yf.Ticker(ticker).history(start=str(start_year) + '-1-1', end=datetime.now().date()).reset_index()

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


def plot_stock(symbol, save_image = 'Y', save_path = save_path):
    
    
    
    earnings = read_query(engine, "select * from awesome.hist_er where ticker = '" + symbol + "' and year(date)> year(curdate())-5 order by date")
    earnings['date'] = pd.to_datetime(earnings['date'], utc=True).dt.date
    
    
    start_year = earnings['date'][0].year
    data = get_stock_data(symbol, start_year)
    data['date'] = pd.to_datetime(data['Date'], utc=True).dt.date
    data.rename(columns = {'Open':'open', 'High':'high', 'Low':'low', 'Close':'close', 'Volume':'volume'}, inplace=True)
    
    zack_rank = read_query(engine, "select zack_rank from awesome.ticker_zack_hist where ticker = '" + symbol + "' order by update_date desc limit 1")
    
    q_list = [] #'2019Q1'
    #logic only applies to data initial from 2019-1-1
    for i in range(len(earnings)):
        
        temp_year = str(earnings['date'][i])[0:4]
        
        if q_list:
            #if current year exist in list then add next quarter
            if q_list[-1][0:4] == temp_year:
                
                q_list.append(q_list[-1][:5]+str(int(q_list[-1][5])+1))
            #if current year doesn't exist add new quarter from 1
            else:
                if q_list[-1][-2:] != 'Q4':    
                    q_list.append(str(int(temp_year)-1)+'Q4')
                else:
                    q_list.append(temp_year+'Q1')
        #initial 2019Q4        
        else:
            q_list.append(str(int(temp_year)-1)+'Q4')
            
    earnings['earning_quarter'] = q_list
    
    earning_info = []
    for i in range(len(earnings)):
        earning_info.append([earnings['date'][i], 
                             earnings['current_close_price'][i], 
                             earnings['earning_quarter'][i] + ' ' + str(zack_rank['zack_rank'][0]) + ' increase' + \
                            str(round((earnings['nextday_close_price'][i]-earnings['current_close_price'][i])/earnings['current_close_price'][i], 4))]
                            )
   
    minor_target_grid = [mdates.date2num(x[0]) for x in earning_info]
    
    #test two yaxis
    fig, ax1 = plt.subplots()
    fig.set_size_inches(25, 12)
    ax2 = ax1.twinx()
    ln1 = ax1.plot(data['date'], data['close'], '-g',label = 'close price')
    ln2 = ax2.plot(data['date'], data['volume'], color = '#C875C4', linestyle = '--',  label = 'volume')
    
    lns = ln1+ln2
    labs = [l.get_label() for l in lns]

    ax1.legend(lns, labs, loc='upper left')
    ax1.set_xlabel('date')
    ax1.set(ylabel = 'close price', title = symbol)
    ax2.set_ylabel('volume')
    
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    #ax1.xaxis.set_minor_locator(mdates.MonthLocator())
    ax1.xaxis.set_minor_locator(ticker.FixedLocator(minor_target_grid))
    
    ax1.xaxis.grid(True, which = 'minor')
    ax1.yaxis.grid()
    ax1.tick_params('x', labelrotation=90)
    ax1.set_ylim(min(data['close'])*0.9, max(data['close'])*1.1)
    ax2.set_ylim(min(data['volume'])*0.9, max(data['volume'])*1.1)
    
    
    
    checkset1 = set()
    for date in earning_info:
        xposition = date[0]
        yposition = date[1]
        

        if xposition in checkset1:
            
            ax1.annotate(date[2], xy=(mdates.date2num(xposition), yposition),
               xycoords='data', ha='center',
               xytext=(0, -20), textcoords='offset points',
               arrowprops=dict(arrowstyle="->",
                               connectionstyle="arc3,rad=0.5",))
        else:
            
            checkset1.add(xposition)
            
            ax1.annotate(date[2], xy=(mdates.date2num(xposition), yposition),
               xycoords='data', ha='center',
               xytext=(0, 20), textcoords='offset points',
               arrowprops=dict(arrowstyle="->",
                               connectionstyle="arc3,rad=0.5",))
    
    
    plt.show()
    if save_image == 'Y':  
       fig.savefig(save_path+symbol + '_' +str(datetime.now().date()) + '_result_visualization.pdf')
     

