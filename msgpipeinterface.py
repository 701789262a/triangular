import datetime
import hashlib
import hmac
import json
import os
from threading import Thread
import errno
import traceback
from urllib.parse import urlencode

import binance.exceptions
import yaml
import numpy
import requests
import unicorn_binance_websocket_api
import zmq
from binance.client import Client
import time
from ipcqueue import posixmq

lotsize = {}
avgtime = [1.0]
ORDER_MARGIN_PRICE_VOLATILITY = 0.03


def pipe_server():
    os.system('python loopcalculator_standalone.py')
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5556")
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

    print("waiting for client")
    q = socket
    p = posixmq.Queue("/orderpipe", maxsize=50)
    try:
        print('starting read')
        while True:
            try:
                start = datetime.datetime.now().timestamp()
                resp = str(q.recv().decode()).strip('\n')
                dict_response = dict(eval(resp))
                if not dict_response['loop'] in loop_list:
                    loop_list.append(dict_response['loop'])
                print('Msg: %s\n| Loop Length: %d | Queue length: %d | Msg rate: %.2f/s' % (
                    resp, len(loop_list), q.qsize(), 1 / (sum(avgtime[-100:]) / 100)))

                if float(dict_response['margin']) > 0:
                    pushqueue = ""
                    start = datetime.datetime.now().timestamp()
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
                            t = Thread(target=instant_execute_trade,
                                       args=(client, real_pair_listed, dict_response, pushqueue, borrowable_qty))
                            t.start()
                            t.join()
                            print(r.text)
                            end = datetime.datetime.now().timestamp()
                            pushqueue.join('[!] took:' + str(end - start) + '\n')

                            msg = "newtrade\n"
                            while p.qsize > 0:
                                msg = msg + p.get() + '\n'

                            with open('instantexecuteerrorlog', 'a') as f:
                                f.write(msg)
                    except TypeError:
                        print('impossibile effettuare loan')
                    with open('logpositive', 'a') as f:
                        f.write(resp + '\n')
                        f.write(response.text + '\n')
                        time.sleep(10)
                        exit()
                finish = datetime.datetime.now().timestamp()
                t = finish - start
                avgtime.append(t)

            except Exception as e:
                with open('errorqueue', 'a') as f:
                    f.write(str(traceback.format_exc()))
                q.close()
                q.unlink()
                p.close()
                p.unlink()
                pipe_server()
    finally:
        q.close()
        q.unlink()


def instant_execute_trade(client, real_pair_listed, dict_response, pushqueue, borrowable_qty):
    prices = dict_response['prices']
    k = 0
    for pair in dict_response['loop']:
        print('[#]Current pair %s' % str(pair))
        if pair[0] + pair[1] in real_pair_listed:
            pushqueue.join(str(round(borrowable_qty * (1 - ORDER_MARGIN_PRICE_VOLATILITY),
                                     lotsize[pair[0] + pair[1] + 'sell'])) + '\n')
            Thread(target=executor_sell, args=(client, pair, borrowable_qty)).start()
            borrowable_qty = borrowable_qty * float(prices[k])
        else:
            pushqueue.join(str(round(borrowable_qty * (1 - ORDER_MARGIN_PRICE_VOLATILITY),
                                     lotsize[pair[1] + pair[0] + 'buy'])) + '\n')
            Thread(target=executor_buy, args=(client, pair, borrowable_qty)).start()
            borrowable_qty = borrowable_qty / float(prices[k])
        k += 1


def executor_buy(client, pair, borrowable_qty):
    q = posixmq.Queue('/' + pair[0] + pair[1])
    for j in range(70):
        qlen = q.qsize()
        print('[#] qlen %d' % qlen)
        if not (qlen > 0):
            Thread(target=execute_trade, args=(client, pair, 'buy', borrowable_qty, j)).start()
            time.sleep(0.015)


def executor_sell(client, pair, borrowable_qty):
    q = posixmq.Queue('/' + pair[0] + pair[1])
    for j in range(70):
        qlen=q.qsize()
        print('[#] qlen %d'%qlen)
        if not (qlen > 0):
            Thread(target=execute_trade, args=(client, pair, 'sell', borrowable_qty, j)).start()
            time.sleep(0.015)


def execute_trade(client, pair, side, borrowable_qty, i):
    q = posixmq.Queue('/' + pair[0] + pair[1])
    try:
        if side == 'sell':
            order = dict(client.order_market_sell(symbol=pair[0] + pair[1], recvWindow=30000,
                                                  quantity=round(borrowable_qty * (1 - ORDER_MARGIN_PRICE_VOLATILITY),
                                                                 lotsize[pair[0] + pair[1] + 'sell'])))
        else:
            order = dict(client.order_market_buy(symbol=pair[1] + pair[0], recvWindow=30000,
                                                 quoteOrderQty=round(
                                                     borrowable_qty * (1 - ORDER_MARGIN_PRICE_VOLATILITY),
                                                     lotsize[
                                                         pair[1] + pair[0] + 'buy'])))
        q.put('ok')
    except binance.exceptions.BinanceAPIException:
        print('[!] No balance or err %s-%d' % (str(pair), i))


if __name__ == "__main__":
    pipe_server()
