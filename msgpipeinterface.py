import datetime
import hashlib
import hmac
import json
import os
import errno
import traceback
from urllib.parse import urlencode
import yaml
import numpy
import requests
import unicorn_binance_websocket_api
from binance.client import Client
import time


lotsize = {}


def pipe_server():
    with open("api.yaml") as f:
        y = yaml.safe_load(f)
        f.close()
    headers = {
        'X-MBX-APIKEY': y['api']
    }
    client = Client(y['api'], y['secret'])
    exchange_info = client.get_exchange_info()['symbols']
    real_pair_listed = [pair['symbol'] for pair in exchange_info]
    for symbol in exchange_info:
        for filter in symbol['filters']:
            if filter['filterType'] == 'LOT_SIZE':
                lot = int(-numpy.log10(float(filter['stepSize'])))
                lotsize[symbol['symbol'] + 'sell'] = lot
        lotsize[symbol['symbol'] + 'buy'] = int(symbol['quotePrecision'])
    loop_list = []
    print("pipe server")
    FIFO = 'looppipe12'
    print("waiting for client")
    try:
        os.mkfifo(FIFO)
        print('1')
    except FileExistsError:
        os.system('rm %s'%FIFO)
        print('2')
        os.mkfifo(FIFO)
        print('3')
    print('4')
    fifo = open(FIFO,'r')
    print('5')
    try:
        while True:
            try:
                print('starting read')
                resp = fifo.read().strip('\n')
                dict_response = dict(eval(resp))
                if not dict_response['loop'] in loop_list:
                    loop_list.append(dict_response['loop'])
                print('Message: %s | Loop Length: %d' % (resp, len(loop_list)))

                if float(dict_response['margin']) > 0:
                    pushqueue=""
                    start=datetime.datetime.now().timestamp()
                    json_data = {
                        'collateralCoin': 'USDT',
                        'loanCoin': dict_response['loop'][0][0],
                        'loanTerm': 7,
                        'collateralAmount': 199,
                    }
                    response = requests.post(
                        'https://www.binance.com/bapi/margin/v1/friendly/collateral/loans/retail/trial-calc-for-borrowing',
                        json=json_data)

                    try:
                        borrowable_qty = float(json.loads(response.text)['data']['limitResult']['loanAmountWanted'])
                        timestamp = int(time.time() * 1000)
                        params = {
                            'loanCoin': dict_response['loop'][0][0],
                            'loanAmount': borrowable_qty,
                            'collateralCoin': 'USDT',
                            'collateralAmount': 200,
                            'loanTerm': 7,
                            'timestamp': timestamp
                        }
                        query_string = urlencode(params)

                        params['signature'] = hmac.new(y['secret'].encode('utf-8'), query_string.encode('utf-8'),
                                                       hashlib.sha256).hexdigest()


                        r = requests.post('https://api.binance.com/sapi/v1/loan/borrow', headers=headers, params=params)
                        if 'coin' not in dict(json.loads(r.text)):
                            pushqueue.join(r.text + '\n')
                            i = 0
                            for pair in dict_response['loop']:
                                if pair[0] + pair[1] in real_pair_listed:
                                    pushqueue.join(str(round(borrowable_qty * 0.99999999,
                                                          lotsize[pair[0] + pair[1] + 'sell'])) + '\n')
                                    order = dict(client.order_market_sell(symbol=pair[0] + pair[1],
                                                                          quantity=round(borrowable_qty * 0.99999999,
                                                                                         lotsize[pair[0] + pair[1] + 'sell'])))
                                    borrowable_qty = float(order['cummulativeQuoteQty'])
                                else:
                                    pushqueue.join(str(round(borrowable_qty * 0.99999999,
                                                          lotsize[pair[1] + pair[0] + 'buy'])) + '\n')
                                    order = dict(client.order_market_buy(symbol=pair[1] + pair[0],
                                                                         quoteOrderQty=round(borrowable_qty * 0.99999999,
                                                                                             lotsize[
                                                                                                 pair[1] + pair[0] + 'buy'])))
                                    borrowable_qty = float(order['executedQty'])
                                i += 1
                            print(r.text)
                            end=datetime.datetime.now().timestamp()
                            pushqueue.join('[!] took:'+str(end-start) + '\n')
                    except TypeError:
                        print('impossibile effettuare loan')
                    with open('logpositive', 'a') as f:
                        f.write(resp + '\n')
                        f.write(response.text + '\n')

            except Exception as e:
                with open('errorqueue', 'a') as f:
                    f.write(str(traceback.format_exc()))
                fifo.close()
                pipe_server()
    finally:
        fifo.close()


if __name__ == "__main__":
    pipe_server()
