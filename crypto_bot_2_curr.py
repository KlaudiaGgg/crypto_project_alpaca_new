

# Import Dependencies
import pandas as pd
import pandas_ta as ta
import alpaca_trade_api as tradeapi
import nest_asyncio
nest_asyncio.apply()
import sqlite3
import pyodbc
from sqlalchemy import create_engine
import urllib
import tracemalloc

tracemalloc.start()


connection = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-CDF9303\MSSQLSERVER01;DATABASE=crypto_database;Trusted_Connection=yes;')
cursor = connection.cursor()
# API Credentials
API_KEY='****'
SECRET_KEY='****'
api = tradeapi.REST(API_KEY, SECRET_KEY,'https://paper-api.alpaca.markets')

# Define crypto related variables
symbol1 = 'BTCUSD'
qty_per_trade = 1

# Check Whether Account Currently Holds Symbol
def check_positions(symbol):
    positions = api.list_positions()
    for p in positions:
        if p.symbol == symbol:
            return float(p.qty)
    return 0


#cursor.execute("DROP TABLE crypto_supertrend_btc")
#cursor.execute("DROP TABLE crypto_supertrend_eth")
#cursor.execute("CREATE TABLE crypto_supertrend_btc (date_time datetime DEFAULT CURRENT_TIMESTAMP, exchange varchar(50),close_price decimal(15,8), SUPERT_7_3 decimal(15,8))")
#cursor.execute("CREATE TABLE crypto_supertrend_eth (date_time datetime DEFAULT CURRENT_TIMESTAMP, exchange varchar(50),close_price decimal(15,8), SUPERT_7_3 decimal(15,8))")


quoted = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=DESKTOP-CDF9303\MSSQLSERVER01;DATABASE=crypto_database")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))



def insert_ticks_btc(tick):
    cursor = connection.cursor()
    try:        
 
        tick.tail(1).to_sql('crypto_supertrend_btc',con = engine,if_exists='append',index=False,method='multi')
        #break
    except Exception as e:
        print(e)
    
    connection.commit()
    #connection.close()

def insert_ticks_eth(tick):
    cursor = connection.cursor()
    try:        
      
        tick.tail(1).to_sql('crypto_supertrend_eth',con = engine,if_exists='append',index=False,method='multi')
        #break
    except Exception as e:
        print(e)
    
    connection.commit()
    #connection.close()


# Supertrend Indicator Bot Function
def supertrend_bot(bar):
    try:
        # Get the Latest Data
        dataframe = api.get_crypto_bars(symbol1, tradeapi.TimeFrame(1, tradeapi.TimeFrameUnit.Minute)).df
        dataframe = dataframe[dataframe.exchange == 'CBSE']
        sti = ta.supertrend(dataframe['high'], dataframe['low'], dataframe['close'], 10, 3)
        dataframe = pd.concat([dataframe, sti], axis=1)     

       
        position = check_positions(symbol=symbol1)
        should_buy = bar['c'] > dataframe["SUPERT_10_3.0"][-1]
        should_sell = bar['c'] < dataframe["SUPERT_10_3.0"][-1]
        print(f"Price: {dataframe.close[-1]}")
        print("Super Trend Indicator: {}".format(dataframe["SUPERT_10_3.0"][-1]))
        print(f"Position: {position} | Should Buy: {should_buy}")
        
        dataframe= dataframe.reset_index()
        dataframe = dataframe.astype({'exchange':'string'})
        
        

        # Check if No Position and Buy Signal is True
        if position == 0 and should_buy == True:
            api.submit_order(symbol1, qty=qty_per_trade, side='buy',type='market',time_in_force='gtc')
            message = f'Symbol: {symbol1} | Side: Buy | Quantity: {qty_per_trade}'
            print(message)
            print(f"Submitting market order for {qty_per_trade} units of {symbol1}") 

        
        if position > 0 and should_buy == True:
            api.submit_order(symbol1, qty=qty_per_trade, side='buy',type='market',time_in_force='gtc')
            available_cash = float(api.get_account().cash)
            message = f'Symbol: {symbol1} | Side: Buy | Quantity: {qty_per_trade}'
            print(message)
            print(f"Submitting market order for {qty_per_trade} units of {symbol1}") 
            

        # Check if Long Position and Sell Signal is True
        if position > 0 and should_sell == True and position<1:
            api.submit_order(symbol1, qty=position, side='sell',type='market',time_in_force='gtc')
            message = f'Symbol: {symbol1} | Side: Sell | Quantity: {position}'
            print(message)
            print(f"Submitting market order for {position} units of {symbol1}") 
            #api.submit_order(symbol, qty_per_trade, 'sell', 'market', 'gtc')

        elif position > 0 and should_sell == True and position>=1:
            api.submit_order(symbol1, qty=qty_per_trade, side='sell',type='market',time_in_force='gtc')
            message = f'Symbol: {symbol1} | Side: Sell | Quantity: {qty_per_trade}'
            print(message)
            print(f"Submitting market order for {qty_per_trade} units of {symbol1}") 
            #api.submit_order(symbol, qty_per_trade, 'sell', 'market', 'gtc')
        

        print("-"*20)
        
        
    except Exception as e:
        print (e)
    #insert_ticks_btc(data_dict)   
    dataframe_new=dataframe[["exchange","close","SUPERT_10_3.0"]]
    dataframe_new=dataframe_new.rename(columns={"close":"close_price","SUPERT_10_3.0":"SUPERT_10_3"})
    #dataframe_new.tail(1).to_sql('crypto_supertrend_btc',con = engine,if_exists='append',index=False,method='multi')
    insert_ticks_btc(dataframe_new)   

        
# Create instance of Alpaca data streaming API
alpaca_stream = tradeapi.Stream(API_KEY, SECRET_KEY, raw_data=True, crypto_exchanges=['CBSE'])

# Create handler for receiving live bar data
async def on_crypto_bar(bar):
    print(bar)
    supertrend_bot(bar)

on_crypto_bar('BTCUSD')

# Subscribe to data and assign handler
alpaca_stream.subscribe_crypto_bars(on_crypto_bar, symbol1)

symbol2='ETHUSD'

# Supertrend Indicator Bot Function
def supertrend_bot2(bar):
    try:
        # Get the Latest Data
        dataframe = api.get_crypto_bars(symbol2, tradeapi.TimeFrame(1, tradeapi.TimeFrameUnit.Minute)).df
        dataframe = dataframe[dataframe.exchange == 'CBSE']
        sti = ta.supertrend(dataframe['high'], dataframe['low'], dataframe['close'], 7, 3)
        dataframe = pd.concat([dataframe, sti], axis=1)

        position = check_positions(symbol=symbol2)
        should_buy = bar['c'] > dataframe["SUPERT_10_3.0"][-1]
        should_sell = bar['c'] < dataframe["SUPERT_10_3.0"][-1]
        print(f"Price: {dataframe.close[-1]}")
        print("Super Trend Indicator: {}".format(dataframe["SUPERT_10_3.0"][-1]))
        print(f"Position: {position} | Should Buy: {should_buy}")
        
        dataframe= dataframe.reset_index()


        # Check if No Position and Buy Signal is True
        if position == 0 and should_buy == True:
            api.submit_order(symbol2, qty=qty_per_trade, side='buy',type='market',time_in_force='gtc')
            message = f'Symbol: {symbol2} | Side: Buy | Quantity: {qty_per_trade}'
            print(message)
            print(f"Submitting market order for {qty_per_trade} units of {symbol2}") 
            
        
        if position > 0 and should_buy == True:
            api.submit_order(symbol2, qty=qty_per_trade, side='buy',type='market',time_in_force='gtc')
            available_cash = float(api.get_account().cash)
            message = f'Symbol: {symbol2} | Side: Buy | Quantity: {qty_per_trade}'
            print(message)
            print(f"Submitting market order for {qty_per_trade} units of {symbol2}") 
            

        # Check if Long Position and Sell Signal is True
        if position > 0 and should_sell == True and position<1:
            api.submit_order(symbol2, qty=position, side='sell',type='market',time_in_force='gtc')
            message = f'Symbol: {symbol2} | Side: Sell | Quantity: {position}'
            print(message)
            print(f"Submitting market order for {position} units of {symbol2}") 
            #api.submit_order(symbol, qty_per_trade, 'sell', 'market', 'gtc')

        elif position > 0 and should_sell == True and position>=1:
            api.submit_order(symbol2, qty=qty_per_trade, side='sell',type='market',time_in_force='gtc')
            message = f'Symbol: {symbol2} | Side: Sell | Quantity: {qty_per_trade}'
            print(message)
            print(f"Submitting market order for {qty_per_trade} units of {symbol2}") 
            #api.submit_order(symbol, qty_per_trade, 'sell', 'market', 'gtc')
        

        print("-"*20)
        
        

    except Exception as e:
        print (e)
      
    dataframe_new=dataframe[["exchange","close","SUPERT_10_3.0"]]
    dataframe_new=dataframe_new.rename(columns={"close":"close_price","SUPERT_10_3.0":"SUPERT_10_3"})

    insert_ticks_eth(dataframe_new)
 

    
# Create handler for receiving live bar data
async def on_crypto_bar(bar):
    print(bar)
    supertrend_bot2(bar)
# Subscribe to data and assign handler
alpaca_stream.subscribe_crypto_bars(on_crypto_bar, symbol2)

# Start streaming of data
alpaca_stream.run()       
        
   
res1=cursor.execute("SELECT * from crypto_supertrend_eth")
res1.fetchall()
