import asyncio
import errno
import logging
import os
import socket
import json
import sys
import queue
import queue
import time
import traceback


import matplotlib.pyplot as plt
import networkx
from networkx import Graph, DiGraph, simple_cycles
import numpy
import tabloo
import pandas as pd
import yaml
import numpy as np
import unicorn_binance_websocket_api
from binance.client import Client
from threading import Thread
import datetime
import polars as pl
import vaex as vx
from pandas import DataFrame
class globalgraph():
    global_graph=False
    global_circular=[]
FEE = 1.00075
zero_trading_fee_promo = ['BUSDUSDT', 'TUSDBUSD', 'TUSDUSDT', 'USDCBUSD', 'USDCUSDT', 'USDPBUSD', 'USDPUSDT']
bitcoin_trading_fee_promo =['BUSDUSDT', 'TUSDBUSD', 'TUSDUSDT', 'USDCBUSD', 'USDCUSDT', 'USDPBUSD', 'USDPUSDT']

FIFO = 'looppipe12'
def main():
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
    tab = {} #dati
    bookdepthdf={} #bookdepth
    graph = {} #grafo
    coinlist = returncoinlist(exchange_info)
    print(len(coinlist))
    for coin1 in coinlist:
        tab[coin1] = {}
        bookdepthdf[coin1] = {}
        for coin2 in coinlist:
            tab[coin1][coin2] = numpy.NAN
            bookdepthdf[coin1][coin2] = numpy.NAN
            pairlist.append(coin1 + '.' + coin2)

    i = 0

    print(len(pairlist))
    bnb_wss_taker = Thread(target=
                           threaded_func,
                           args=(tab,pairlist, graph,bookdepthdf))
    bnb_wss_taker.start()
    pairlist = []
    i = 0
    Thread(target=df_displayer, args=(tab, graph)).start()
    Thread(target=grapher,args=(graph,)).start()
    time.sleep(100)
    print(real_pair_listed)
    triangle_calculator(tab,graph, real_pair_listed,bookdepthdf)


def returncoinlist(exchangeinfo):
    partial_list = []
    for pair in exchangeinfo:
        partial_list.append(pair['baseAsset'])
        partial_list.append(pair['quoteAsset'])
    return list(set(partial_list))

def triangle_calculator(df,graph,pairlist,bookdepthdf):
    while True:
        print('graph2',graph)
        if not globalgraph.global_graph or globalgraph.global_graph!=graph:
            G = Graph(graph)
            labels = dict(zip(G.nodes(), G.nodes()))
            networkx.draw_networkx(G, labels=labels)
            DG = DiGraph(G)
            circular = list(simple_cycles(DG))
            closed_loop_list = [loop for loop in circular if len(loop) == 3]
            globalgraph.global_graph=graph
            globalgraph.global_circular=circular
        else:
            closed_loop_list = [loop for loop in globalgraph.global_circular if len(loop) == 3]
        print("loops max 3 found",closed_loop_list)
        #loop_calculator(df,closed_loop_list[0],pairlist,handle)
        for loop in closed_loop_list:
            Thread(target=loop_calculator,args=(df,loop,pairlist,bookdepthdf)).start()

def loop_calculator(df,loop,pairlist,bookdepthdf):
    try:
        """['ETH', 'BTC', 'EUR']  =>  ["ETHBTC", "BTCEUR", "EURETH"]"""
        pairs = [[loop[0],loop[1]],[loop[1],loop[2]],[loop[2],loop[0]]]
        prices=[]
        depths=[]
        margin =0.0
        for pair in pairs:
            if isfloat(df[pair[0]][pair[1]]):
                print("Testing",pair[0]+pair[1])
                if pair[0]+pair[1] in pairlist:
                    margin += np.log(float(df[pair[1]][pair[0]]))
                    depths.append(bookdepthdf[pair[1]][pair[0]])
                    prices.append(df[pair[1]][pair[0]])
                    if pair[0]+pair[1] in zero_trading_fee_promo or pair[1]+pair[0] in zero_trading_fee_promo or pair[0]+pair[1] in bitcoin_trading_fee_promo or pair[1]+pair[0] in bitcoin_trading_fee_promo:
                        margin-= 0
                    else:
                        margin-= 0.00075
                else:
                    margin += -np.log(float(df[pair[1]][pair[0]]))
                    depths.append(bookdepthdf[pair[1]][pair[0]])
                    prices.append(df[pair[1]][pair[0]])
                    if pair[0]+pair[1] in zero_trading_fee_promo or pair[1]+pair[0] in zero_trading_fee_promo or pair[0]+pair[1] in bitcoin_trading_fee_promo or pair[1]+pair[0] in bitcoin_trading_fee_promo:
                        margin-= 0
                    else:
                        margin-= 0.00075
        print("Loop %s Margin %f%%"%(str(loop),margin*100))
        api_message_push = {'loop':pairs,'margin':round(margin*100,5),'prices':prices,'depths':depths,'timestamp':int(datetime.datetime.now().timestamp())}
        os.system('echo %s > %s'%(str(api_message_push),FIFO))
    except Exception as e:
        with open('culo.txt','a') as f:
            f.write(str(traceback.format_exc()))

def pair_list_slimmer(pair_list, pair):
    new_pair_list = []
    for second_pair in pair_list:
        if pair.split('.')[0] in second_pair or pair.split('.')[1] in second_pair:
            new_pair_list.append(second_pair)
    new_pair_list.remove(pair)
    return new_pair_list

def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

def grapher(graph):
    print('Grapher started...')
    while True:
        G=Graph(graph)
        labels = dict(zip(G.nodes(),G.nodes()))
        networkx.draw_networkx(G,labels=labels)
        DG = DiGraph(G)
        plt.show()


def df_displayer(df, graph):
    while True:
        os.system('cls')
        print(pd.DataFrame.from_dict(df))
        time.sleep(0.2)


def subscribe_wss(api_manager, pairlist):
    stream=[item.lower().replace('.','') for item in pairlist]
    print('sublen %d firstdata %s'%(len(stream),stream[0]))
    api_manager.create_stream(channels=['bookTicker'],markets=stream)


def threaded_func(df, pairlist, graph,bookdepthdf):
    print('Starting WSS connection')
    binance_websocket_api_manager = unicorn_binance_websocket_api.BinanceWebSocketApiManager(exchange="binance.com")
    withoutpoint_topoint = dict()
    Thread(target=subscribe_wss, args=(binance_websocket_api_manager, pairlist)).start()
    for pair in pairlist:
        print('Create stream for %s' % (pair.replace('.', '')))
        withoutpoint_topoint[pair.replace('.', '')] = pair
    while True:
        try:
            oldest_stream_data_from_stream_buffer = binance_websocket_api_manager \
                .pop_stream_data_from_stream_buffer()
            if oldest_stream_data_from_stream_buffer:
                res_bnb = oldest_stream_data_from_stream_buffer
                if 'result' not in res_bnb:
                    # conn.sendall(res_bnb.encode())
                    # print(res_bnb)
                    df[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[0]][
                        withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                            1]] = json.loads(res_bnb)['data']['a']
                    df[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[1]][
                        withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                            0]] = json.loads(res_bnb)['data']['b']
                    bookdepthdf[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[0]][
                        withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                            1]] = json.loads(res_bnb)['data']['A']
                    bookdepthdf[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[1]][
                        withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                            0]] = json.loads(res_bnb)['data']['B']
                    try:
                        if not graph[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[0]].__contains__(
                                withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                                    1]):
                            graph[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[0]].append(
                                withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                                    1])
                    except:
                        graph[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[0]] = []
            else:
                continue
        except:
            continue


def log(trace, print_flag=False):
    if print_flag:
        print(trace)
    with open('emakerlog', 'a+') as file:
        file.write(
            str(datetime.datetime.now().astimezone()) + " VVVVVVVVVVVVVVVVVVVVVVV\n" + trace + '\n\n')


if __name__ == "__main__":
    print('ok')
    #sys.stderr = object
    q1 = queue.Queue()
    main()
