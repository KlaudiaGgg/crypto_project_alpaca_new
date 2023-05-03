

# Import Dependencies
import pandas as pd
import alpaca_trade_api as tradeapi
import datetime as dt
import nest_asyncio
nest_asyncio.apply()
import json
import requests
import time
starttime = time.time()
from alpaca_trade_api import REST
import os 
from sqlalchemy import create_engine
import urllib
import pyodbc
from pandas import json_normalize

# Trusted Connection to Named Instance
connection = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-CDF9303\MSSQLSERVER01;DATABASE=crypto_database;Trusted_Connection=yes;')
cursor = connection.cursor()

os.chdir('C:/Users/User/Documents/Magister')

headers = json.loads(open("key.txt",'r').read())
endpoint = "https://paper-api.alpaca.markets"


#Tworzenie tabeli
#cursor.execute("DROP TABLE crypto_cash")
#cursor.execute("DROP TABLE crypto_cash")
#cursor.execute("CREATE TABLE crypto_positions (date_time datetime DEFAULT CURRENT_TIMESTAMP, symbol varchar(50), exchange VARCHAR(50), qty decimal(15,8), side varchar(50), market_value decimal(15,8), cost_basis decimal(15,8), unrealized_pl decimal(15,8), unrealized_plpc decimal(15,8),current_price decimal(15,8), lastday_price decimal(15,8), change_today decimal(15,8))")
#cursor.execute("CREATE TABLE crypto_cash (date_time datetime DEFAULT CURRENT_TIMESTAMP,available_cash decimal(15,8))")


API_KEY='****'
SECRET_KEY='****'


quoted = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=DESKTOP-CDF9303\MSSQLSERVER01;DATABASE=crypto_database")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))


def insert_ticks_positions(tick):
    cursor = connection.cursor()
    try:        

            print("---")
            vals = [tick[0]["symbol"],tick[0]["exchange"],tick[0]["qty"],tick[0]["side"],tick[0]["market_value"],tick[0]["cost_basis"],tick[0]["unrealized_pl"],tick[0]["unrealized_plpc"],tick[0]["current_price"],tick[0]["lastday_price"],tick[0]["change_today"]]
            query = "INSERT INTO crypto_positions (symbol,exchange,qty,side,market_value,cost_basis,unrealized_pl,unrealized_plpc,current_price,lastday_price,change_today) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
            vals2 =[tick[1]["symbol"],tick[1]["exchange"],tick[1]["qty"],tick[1]["side"],tick[1]["market_value"],tick[1]["cost_basis"],tick[1]["unrealized_pl"],tick[1]["unrealized_plpc"],tick[1]["current_price"],tick[1]["lastday_price"],tick[1]["change_today"]]
            cursor.execute(query,vals)
            cursor.execute(query,vals2)
    except Exception as e:
        print(e)
    
    connection.commit()

    
def insert_ticks_cash(tick):
    cursor = connection.cursor()
    try:        
        vals = [float(tick.available_cash)]
        query = "INSERT INTO crypto_cash (available_cash) VALUES (?)"
        cursor.execute(query,vals) 
    except Exception as e:
        print(e)
    
    connection.commit()


while True:
    def positions(symbol=""):
        if len(symbol)>1:
            pos_url = endpoint + "/v2/positions/{}".format(symbol)
        else:
            pos_url = endpoint + "/v2/positions"
        r = requests.get(pos_url, headers=headers).json()
        
        if len(r)==0:
            r = dict({'symbol': 'BTCUSD' ,'exchange': 'None','qty': 0,'side':'None','market_value':0,'cost_basis':0,'unrealized_pl':0,'unrealized_plpc':0,'current_price':0,'lastday_price':0,'change_today':0})
            r2 = dict({'symbol': 'ETHUSD' ,'exchange': 'None','qty': 0,'side':'None','market_value':0,'cost_basis':0,'unrealized_pl':0,'unrealized_plpc':0,'current_price':0,'lastday_price':0,'change_today':0})
            r=(r,r2)

        else: 
            r=r

            r=(r[0],r[len(r)-1])
            

        return r

    print(positions())
    print('-----')

    insert_ticks_positions(positions())
    rest_api=REST(API_KEY, SECRET_KEY, 'https://paper-api.alpaca.markets')
    available_cash = float(rest_api.get_account().cash)
    available_cash_df=pd.DataFrame([available_cash],columns=['available_cash'])
    insert_ticks_cash(available_cash_df)
    time.sleep(60.0 - ((time.time() - starttime) % 60.0))



#check, co znajduje się w tabeli 
res2=cursor.execute("SELECT * from crypto_positions")
res2.fetchall()



    
        
        
