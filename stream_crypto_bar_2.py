

import websocket
import os
import json
import datetime as dt
import pandas as pd 
import pyodbc
from sqlalchemy import create_engine
import urllib



os.chdir('C:/Users/User/Documents/Magister')
endpoint = "wss://stream.data.alpaca.markets/v1beta2/crypto"
headers = json.loads(open("key_2.txt",'r').read())


connection = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-CDF9303\MSSQLSERVER01;DATABASE=crypto_database;Trusted_Connection=yes;')
cursor = connection.cursor()


quoted = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=DESKTOP-CDF9303\MSSQLSERVER01;DATABASE=crypto_database")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

#cursor.execute("DROP TABLE crypto_stream_bar")
#cursor.execute("CREATE TABLE crypto_stream_bar (date_time datetime DEFAULT CURRENT_TIMESTAMP,symbol varchar(50),open_price decimal(15,8),high decimal(15,8),low decimal(15,8),close_price decimal(15,8),volume decimal(15,8))")


#############

def insert_ticks(tick):
    cursor = connection.cursor()
    try:        
        vals = [tick[0]["S"],tick[0]["o"],tick[0]["h"],tick[0]["l"],tick[0]["c"],tick[0]["v"]]
        query = "INSERT INTO crypto_stream_bar (symbol,open_price,high,low,close_price,volume) VALUES (?,?,?,?,?,?)"
        cursor.execute(query,vals) 
        #break
    except Exception as e:
        print(e)
    
    connection.commit()
 

def on_open(ws):
    auth = {"action": "auth", "key": headers["APCA-API-KEY-ID"], "secret": headers["APCA-API-SECRET-KEY"]}
    
    ws.send(json.dumps(auth))
    
    message = {"action":"subscribe","bars": ["BTC/USD", "ETH/USD"]}
    #message = {"action":"subscribe","bars": ["BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD", "LTC/USD", "USDT/USD", "SHIB/USD","MATI/USD","ALGO/USD","AVAX/USD","LINK/USD"]}
                
    ws.send(json.dumps(message))

def on_message(ws, message):
    print(message)
    tick = json.loads(message)
    insert_ticks(tick)


ws = websocket.WebSocketApp(endpoint, on_open=on_open, on_message=on_message)
ws.run_forever()



res1=cursor.execute("SELECT * from crypto_stream_bar")
res1.fetchone()
res1.fetchall()


