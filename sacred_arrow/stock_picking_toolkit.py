import pandas as pd
from sqlalchemy import create_engine
import datetime as dt
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import time as t
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
from urllib.parse import quote_plus as urlquote
import yfinance as yf
import matplotlib.pyplot as plt
import seaborn as sns
import random
import numpy as np
import pandas_datareader as pdr
from urllib.request import urlopen, Request

symbol_list = ['TSLA', 'DXCM', 'AMZN', 'PLTR', 'COIN', 'NFLX', 'ROKU', 'LSCC']



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

test = transaction_tracker(['TSLA', 'DXCM', 'QCOM', 'PDD', 'LSCC'])

test_again = test.transpose()

'''

'''
def weekly_change(ticker:str, start_year = 2021):
    '''
    target year to now, weekly stock price difference
    '''
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
    
    return last_rows 

'''
fa info for sp500
'''
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
    _host = '127.0.0.1'
    _db = 'awesome'
    _user = 'root'
    _password = 'Albert@25'
    engine = create_engine('mysql://'+_user+':'+urlquote(_password)+'@'+_host)
    
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
    #ticker_list  = read_query(engine, "select symbol as ticker from awesome.company_info")['ticker'].tolist()
    
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    sp500 = table[0]
    
    df = pd.DataFrame(index=sp500['Symbol'].tolist(),columns=metric)
    df = get_fundamental_data(df)
    
    refine_df = df.loc[~df['P/B'].isnull() ].reset_index()
    refine_df.rename(columns = {'index':'ticker', 'P/B': 'PB', 'P/E': 'PE', 'Debt/Eq':'Debt_Eq'}, inplace=True)
    
    result = pd.merge(refine_df, sp500, left_on = 'ticker', right_on = 'Symbol', how = 'left')
    result.to_sql(name='sp500_fa', con=engine, schema = 'awesome', if_exists='replace', index = False)



