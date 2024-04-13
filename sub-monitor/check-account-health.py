import time
from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from dotenv import load_dotenv
import argparse 
import socket
import os 
import json
import psycopg2
from time import sleep
from datetime import datetime ,timedelta
load_dotenv()
conn= psycopg2.connect(
    dbname=os.environ.get("DB_NAME"),
    user=os.environ.get("DB_USER")
)
cur = conn.cursor()

def send_msg(to:str, msg:str,silent=False):
    print("send MSG ")
    print(to,msg,silent)
    path = os.environ.get("SOCKET_PATH","/var/run/tg-notifier/tg-notifier.sock")
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(path)
    data = dict (
        to = to,
        msg = msg,
        silent=silent
    )
    s.send(json.dumps(data).encode('ascii'))
    s.close()


def get_account(account_name='PrimaryArbitage'):
    # Key, Secret Exchange, Account
    cur.execute(f"select api_key, api_secret, exchange, account_name from account_info where account_name= %s and exchange = 'Binance'",(account_name,))
    return cur.fetchall()

class BinanceMonitor: 
    def __init__(self,key,secret):
        self.http_client= UMFutures(key=key,secret=secret)
        # self.listen_key = self.http_client.new_listen_key()
        self.last_notified= datetime.now()  - timedelta(days=3) # Initial notified
        # self.ws_client = UMFuturesWebsocketClient(on_message=callback)
    def check_and_notified(self,thres=0.1):
        result=  self.http_client.account()
        maint=result['totalMaintMargin']
        balance=result['totalMarginBalance']
        risk_ratio = float(maint) / float(balance)
        msg = f"[{datetime.now()}]Maint: {maint}, Balance: {balance}, Risk Ratio: {risk_ratio*100:.4f}%"
        if isRisky:= risk_ratio > thres:
            msg = f"RISKY!!!!!: " + msg

        ##Logging and notifying
        print(msg)
        if  isRisky or (datetime.now() - self.last_notified).total_seconds() //3600 > 8: # More than 8 hour
            send_msg('www10177',f'{msg}',silent=not isRisky)
            self.last_notified = datetime.now()
        
l = get_account()
key,pwd = l[0][0],l[0][1]
bm = BinanceMonitor(key,pwd)

while True: 
    try:
        bm.check_and_notified()
    except Exception as e :
        print(e)
        send_msg('www10177',f'Broken{e}',False)
    sleep(10)

