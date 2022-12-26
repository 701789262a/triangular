import gc
import json
import queue
import time
import traceback
from ipcqueue import posixmq
import math
import matplotlib.pyplot as plt
import networkx
from networkx import Graph, DiGraph, simple_cycles
import numpy
import yaml
import unicorn_binance_websocket_api
from binance.client import Client
from threading import Thread
import datetime
import pandas as pd
class globalgraph():
    global_graph=False
    global_circular=[]

FEE = 1.00075
zero_trading_fee_promo = ['BUSDUSDT', 'TUSDBUSD', 'TUSDUSDT', 'USDCBUSD', 'USDCUSDT', 'USDPBUSD', 'USDPUSDT']
bitcoin_trading_fee_promo =['BUSDUSDT', 'TUSDBUSD', 'TUSDUSDT', 'USDCBUSD', 'USDCUSDT', 'USDPBUSD', 'USDPUSDT']
GCCOUNTER_THRESHOLD=600000
FIFO = '/looppipe12'
def returncoinlist(exchangeinfo):
    partial_list = []
    for pair in exchangeinfo:
        partial_list.append(pair['baseAsset'])
        partial_list.append(pair['quoteAsset'])
    return list(set(partial_list))
def main():
    gc.enable()
    with open("api.yaml") as f:
        y = yaml.safe_load(f)
        f.close()
    api_key = y['api']
    api_secret = y['secret']
    pairlist = []
    client = Client(api_key, api_secret)
    exchange_info = client.get_exchange_info()['symbols']
    real_pair_listed = [pair['symbol'] for pair in exchange_info]
    exchange_info = exchange_info[:50]
    print(len(exchange_info))
    exchange_info = [item for item in exchange_info if 'EUR' not in item['symbol']]
    print(len(exchange_info))
    coinlist = returncoinlist(exchange_info)
    print(len(coinlist))
    time.sleep(5)
    # Initialize data structures
    df = pd.DataFrame(index=coinlist, columns=coinlist)
    df.fillna(numpy.NAN, inplace=True)
    bookdepthdf = pd.DataFrame(index=coinlist, columns=coinlist)
    bookdepthdf.fillna(numpy.NAN, inplace=True)
    graph = {}
    pairlist = []
    for coin1 in coinlist:
        for coin2 in coinlist:
            pairlist.append(coin1 + '.' + coin2)

    print(len(pairlist))
    bnb_wss_taker = Thread(target=threaded_func, args=(df, pairlist, graph, bookdepthdf))
    bnb_wss_taker.start()
    i = 0
    q=posixmq.Queue(FIFO)
    Thread(target=grapher, args=(graph,)).start()
    time.sleep(100)
    print(real_pair_listed)
    triangle_calculator(df, graph, real_pair_listed, q, bookdepthdf)
def triangle_calculator(df, graph, pairlist, q, bookdepthdf):
    gccounter = 0
    while True:
        if q.qsize() > 5:
            print('[!] Queue getting full! Waiting 2 secs...')
            time.sleep(2)
            continue
        print('graph2', graph)
        if not globalgraph.global_graph or globalgraph.global_graph != graph:
            print("[!] Redrawing graph...")
            G = Graph(graph)
            labels = dict(zip(G.nodes(), G.nodes()))
            networkx.draw_networkx(G, labels=labels)
            DG = DiGraph(G)
            circular = list(simple_cycles(DG))
            closed_loop_list = [loop for loop in circular if len(loop) == 3]
            globalgraph.global_graph = graph
            globalgraph.global_circular = circular
            globalgraph.global_closed_loop_list = closed_loop_list
            print(closed_loop_list)
        i = 0
        j = 0
        for closed_loop in closed_loop_list:
            pair1 = closed_loop[0] + '.' + closed_loop[1]
            pair2 = closed_loop[1] + '.' + closed_loop[2]
            pair3 = closed_loop[2] + '.' + closed_loop[0]
            try:
                pair1_data = df[pair1][0]
                pair2_data = df[pair2][0]
                pair3_data = df[pair3][0]
            except KeyError:
                continue
            if pair1_data is not numpy.NAN and pair2_data is not numpy.NAN and pair3_data is not numpy.NAN:
                if numpy.isnan(pair1_data) or numpy.isnan(pair2_data) or numpy.isnan(pair3_data):
                    continue
                print(pair1_data)
                print(pair2_data)
                print(pair3_data)
                pair1_bid, pair1_ask = pair1_data['bid'], pair1_data['ask']
                pair2_bid, pair2_ask = pair2_data['bid'], pair2_data['ask']
                pair3_bid, pair3_ask = pair3_data['bid'], pair3_data['ask']
                midpair1 = (pair1_bid + pair1_ask) / 2
                midpair2 = (pair2_bid + pair2_ask) / 2
                midpair3 = (pair3_bid + pair3_ask) / 2
                if midpair1 > midpair2 * midpair3:
                    print('arbitrage opportunity')
                    triangle_profit = midpair1 * (1 - FEE) * (1 - FEE) * (1 - FEE) - midpair2 * midpair3
                    triangle_profit = triangle_profit / midpair1
                    print('triangle profit', triangle_profit)
                    if triangle_profit > 0.0005:
                        print('[+] Triangle profit: ', triangle_profit)
                        print(pair1, pair2, pair3)
                        q.put({"pair1": pair1, "pair2": pair2, "pair3": pair3, "triangle_profit": triangle_profit,
                               "midpair1": midpair1, "midpair2": midpair2, "midpair3": midpair3})
                    else:
                        print('[-] Triangle profit too low')
                elif midpair2 > midpair3 * midpair1:
                    print('arbitrage opportunity')
                    triangle_profit = midpair2 * (1 - FEE) * (1 - FEE) * (1 - FEE) - midpair1 * midpair3
                    triangle_profit = triangle_profit / midpair2
                    print('triangle profit', triangle_profit)
                    if triangle_profit > 0.0005:
                        print('[+] Triangle profit: ', triangle_profit)
                        print(pair2, pair3, pair1)
                        q.put({"pair1": pair2, "pair2": pair3, "pair3": pair1, "triangle_profit": triangle_profit,
                               "midpair1": midpair2, "midpair2": midpair3, "midpair3": midpair1})
                    else:
                        print('[-] Triangle profit too low')
                elif midpair3 > midpair1 * midpair2:
                    print('arbitrage opportunity')
                    triangle_profit = midpair3 * (1 - FEE) * (1 - FEE) * (1 - FEE) - midpair1 * midpair2
                    triangle_profit = triangle_profit / midpair3
                    print('triangle profit', triangle_profit)
                    if triangle_profit > 0.0005:
                        print('[+] Triangle profit: ', triangle_profit)
                        print(pair3, pair1, pair2)
                        q.put({"pair1": pair3, "pair2": pair1, "pair3": pair2, "triangle_profit": triangle_profit,
                               "midpair1": midpair3, "midpair2": midpair1, "midpair3": midpair2})
                    else:
                        print('[-] Triangle profit too low')
                else:
                    print('no arbitrage opportunity')
            else:
                print('[-] Data not available')
            print('[+] GCCOUNTER: ', gccounter)
            gccounter += 1
            if gccounter >= GCCOUNTER_THRESHOLD:
                gc.collect()
                gccounter = 0
            time.sleep(2)
def threaded_func(df, pairlist, graph, bookdepthdf):
    ws = unicorn_binance_websocket_api.BinanceWebSocketApiManager()
    ws.start_multiplex_socket(pairlist, process_message)
    ws.start()

def process_message(msg, df,bookdepthdf):
    try:
        if msg['e'] == 'error':
            print('[-] Error: ', msg)
        elif 'depthUpdate' in msg['e']:
            symbol = msg['s']
            bids = msg['b']
            asks = msg['a']
            for bid in bids:
                price = float(bid[0])
                qty = float(bid[1])
                bookdepthdf[symbol]['bid'][price] = qty
            for ask in asks:
                price = float(ask[0])
                qty = float(ask[1])
                bookdepthdf[symbol]['ask'][price] = qty
        elif '24hrTicker' in msg['e']:
            data = msg['p']
            symbol = msg['s']
            df[symbol] = {}
            df[symbol]['bid'] = float(data)
            df[symbol]['ask'] = float(data)
            df[symbol]['last_price'] = float(data)
            df[symbol]['quote_volume'] = float(data)
            df[symbol]['trades'] = int(data)
            df[symbol]['taker_buy_base_asset_volume'] = float(data)
            df[symbol]['taker_buy_quote_asset_volume'] = float(data)
    except Exception as e:
        print(e)
        print('[-] Exception in process_message: ', traceback.format_exc())

def grapher(graph):
    while True:
        try:
            for pair in graph:
                for key in graph[pair]:
                    graph[pair][key] = graph[pair][key] * 0.99
            time.sleep(60)
        except Exception as e:
            print(e)
            print('[-] Exception in grapher: ', traceback.format_exc())
            time.sleep(60)

if __name__ == "__main__":
    main()

