# -*- coding: utf-8 -*-
"""
Created on Thu Jan  6 14:52:44 2022

@author: tsenh

notes update weekly
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
from urllib.request import urlopen, Request
import yahoo_fin.stock_info as si
import yfinance as yf
import csv
from single_stock_analysis import plot_stock


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
# This funtion scrap each Symbol page and extract the Zacks Rank
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
       
def fundamental_metric(soup, metric):
    return soup.find(text = metric).find_next(class_='snapshot-td2').text


       

ticker  = read_query(engine, 'SELECT distinct ticker FROM awesome.hist_er_elite where date_add(date, interval 120 day)> sysdate()')
#zack records backup
elite_ticker_list = read_query(engine, 'SELECT distinct ticker FROM awesome.hist_er_elite')

prev_next_er = read_query(engine, 'select a.ticker, a.zack_rank as prev_zack_rank from( \
select ticker, zack_rank, update_date, rank() over (partition by ticker order by update_date desc) as order_no \
from awesome.ticker_zack_hist) a \
where a.order_no =1')


zack_rank = []
institutional_holder = []
Close = []

tt = t.time()
for i in range(len(elite_ticker_list)):
    t0 =  t.time()
    temp_ticker = elite_ticker_list['ticker'][i]
    '''
    if i%5==0:
        t.sleep(5)
    '''
    try:
        zack_rank.append(zacks_rank(temp_ticker))
        print('{0} takes {1} seconds for zack info'.format(temp_ticker , round(t.time()-t0,2)))
    except:
        zack_rank.append('')
        print('{0} has no zack info'.format(temp_ticker))
        
    t1 = t.time()  
    try:
        #http://finviz.com/quote.ashx?t=tsla
        url = ("http://finviz.com/quote.ashx?t=" + temp_ticker.lower())
        req = Request(url=url,headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}) 
        response = urlopen(req)
        soup = BeautifulSoup(response ,'html.parser')
        '''
        institutional_holder.append(fundamental_metric(soup,'Inst Own'))
        
        '''
        inst_own = fundamental_metric(soup,'Inst Own')
        if inst_own == '-':
            try: 
                
                temp_tick = yf.Ticker(temp_ticker)
                institutional_holder.append(temp_tick.major_holders[0][1])
            except:
                institutional_holder.append('')
        else:
            institutional_holder.append(fundamental_metric(soup,'Inst Own'))
            
        Close.append(fundamental_metric(soup,'Price'))
        print('{0} takes {1} seconds for institutional and close price info'.format(temp_ticker , round(t.time()-t1,2)))
    except Exception as e:
        institutional_holder.append('')
        Close.append(np.nan)
        print (temp_ticker, 'not found')
        
print('All takes {0} seconds'.format(round(t.time()-tt,2)))

ticker_zack_hist = pd.DataFrame({'ticker':elite_ticker_list['ticker'], 'zack_rank':zack_rank, 'institutional_hold':institutional_holder})

ticker_zack_hist['update_date'] = dt.datetime.now().date()
ticker_zack_hist['Close'] = Close

ticker_zack_hist['institutional_hold'] = ticker_zack_hist['institutional_hold'].apply(lambda x: x.replace(',','')  if (x!='' and not type(x) == np.float64) else np.nan)
ticker_zack_hist['institutional_hold'] = ticker_zack_hist['institutional_hold'].apply(
    lambda x: round(float(x.split('%')[0]) / 100, 4) if (x != '' and not isinstance(x, np.float64) and not isinstance(x, float)) else np.nan
)
ticker_zack_hist['Close'].replace('-', np.nan, inplace=True)
ticker_zack_hist['Close'] = ticker_zack_hist['Close'].astype(float)
ticker_zack_hist['zack_rank'].replace('', np.nan, inplace=True)

ticker_zack_hist.to_sql(name='ticker_zack_hist', con=engine, schema='awesome', if_exists='append', index=False)


ticker = pd.merge(ticker, ticker_zack_hist[['ticker', 'zack_rank']], how='left', on='ticker')

#er date
CSV_URL = 'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&horizon=3month&apikey=DHXUWE4M05O9WHWW'

with requests.Session() as s:
    download = s.get(CSV_URL)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    
er_date = pd.DataFrame(my_list[1:])
er_date = er_date.rename(columns={0:'ticker', 1:'company name', 2:'er_date', 3:'fiscalDateEnding', 4:'estimate', 5:'currency'})

er_date_dedu = er_date[['ticker','er_date']].groupby('ticker').first().reset_index()

ticker_merge = pd.merge(ticker, er_date_dedu, how = 'inner', left_on='ticker', right_on='ticker')
ticker_merge['er_date'] = pd.to_datetime(ticker_merge['er_date'])
#select useful ticker info


cur_elite_ticker  = pd.merge(ticker_merge, prev_next_er, how = 'left', on ='ticker')
cur_elite_ticker = cur_elite_ticker[['ticker', 'er_date', 'zack_rank', 'prev_zack_rank']]
cur_elite_ticker.to_sql(name='next_er_date', con=engine, schema = 'awesome', if_exists='replace', index = False)


text = '\
select distinct ticker, er_date, zack_rank, prev_zack_rank, accurate_pct, avg_change, institutional_hold,  close as current_close, sector, industry, company_name, level, comment from ( \
select elite.ticker, trend.accurate_pct, avg_change.avg_change, temp.er_date, temp.zack_rank, temp.prev_zack_rank, zack.institutional_hold, zack.close, com.sector, com.industry, com.company_name, \
cf.level, cf.comment \
from awesome.hist_er_elite elite \
left join ( \
select aa.ticker, sum(aa.same_direction)/count(*) as accurate_pct \
from ( \
select ticker, \
case \
when epssurprisepct < 0 and price_change >0 then 0 \
when epssurprisepct > 0 and price_change <0 then 0 \
else 1 end as same_direction \
from awesome.hist_er_elite \
) aa \
group by aa.ticker \
) trend on trend.ticker = elite.ticker \
left join (  \
select ticker, avg(abs(price_change)) as avg_change \
from awesome.hist_er_elite  \
group by ticker) avg_change on avg_change.ticker = elite.ticker \
left join awesome.next_er_date temp on temp.ticker = elite.ticker \
left join awesome.company_info com on com.Symbol = elite.ticker \
left join (select * from (\
select ticker, institutional_hold, close, rank() over (partition by ticker order by update_date desc) as rank_date from awesome.ticker_zack_hist) t \
where t.rank_date =1) \
	zack on zack.ticker = elite.ticker \
left join awesome.company_freedom cf on cf.Ticker = elite.Ticker \
where temp.er_date is not null and avg_change>0.08 \
and temp.zack_rank =1 and temp.prev_zack_rank -temp.zack_rank <=2 \
order by temp.er_date, elite.ticker, elite.date \
) as a order by er_date \
' 
df = read_query(engine, text)

df['etl_date'] = dt.datetime.now().date()

df.to_sql(name = 'stock_er_weekly_update', con = engine, schema = 'awesome', if_exists = 'append', index = False)

for i in range(len(df)):
    try:
        plot_stock(df['ticker'][i])
    
    except:
        print('stock {0} cannot be found.'.format(df['ticker'][i]))
    
    