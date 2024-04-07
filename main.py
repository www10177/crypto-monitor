from binance.um_futures import UMFutures
from binance.spot import Spot
from dotenv import load_dotenv
import os 
import json
import duckdb
import socket
from datetime import datetime
import asyncio
import time
import requests
load_dotenv()
db= duckdb.connect(os.environ.get("DUCKDB_PATH"))

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
    
def get_binance_futures(key,secret)->list:
    bnb_um_client= UMFutures(key = key, secret=secret)
    result = [ ]
    holdings = [pos for pos in bnb_um_client.get_position_risk() if float(pos['positionAmt']) != 0.0]
    startTime= 1000*(int(time.time()) - 3600*3)#last three hour in milli epoch time
    all_income = bnb_um_client.get_income_history(startTime=startTime,limit=300,incomeType='FUNDING_FEE')
    for pos in holdings:
        symbol = pos['symbol']
        for row in all_income :
            if row['symbol'] == symbol :
                # time = time.strftime("%H:%M", time.localtime(row['time']/1000)) # ms to second
                income = row['income']
        result.append(dict( symbol=symbol ,income=float(income)))
    
    return result


def get_futures(username): 
    account_infos= db.execute(f"select APIKey, APISecret, Exchange, AccountName from AccountInfo where TGUserName == ?",[username]).fetchall()
    msg = ""
    all_profit= 0
    for (key,secret,exchange, name) in account_infos:
        sub_profit=0
        if exchange == 'Binance':
            all_symbol_result = get_binance_futures(key,secret)
            
            #If position existed in this account 
            if len(all_symbol_result) >0:
                msg += f"[Binance]{name}\n"
                #Get all trading pairs
                for result in all_symbol_result:
                    msg += f"{result['symbol']}: {result['income']:.3f}\n"
                    sub_profit += result['income']

                msg += f"Sub-Profit : {sub_profit:.4f}\n"
                msg += '=====\n'
                all_profit += sub_profit
    msg += f"All Profit: {all_profit:.3f}"
    return msg
        
                
                
def main():
    tg_username = os.environ['TG_USERNAME']
    now = datetime.now()
    if now.hour > 0 and now.hour < 7:
        silent = True
    else :
        silent = False
    msg = now.strftime("%m/%d %H:%M") + '\n'
    msg += get_futures(tg_username)
    send_msg(to=tg_username,msg=msg,silent=silent)

if __name__ == "__main__":
    main()