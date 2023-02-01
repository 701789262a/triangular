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
class globalgraph():
    global_graph=False
    global_circular=[]
    FEE = 1.00075
    zero_trading_fee_promo = ['BUSDUSDT', 'TUSDBUSD', 'TUSDUSDT', 'USDCBUSD', 'USDCUSDT', 'USDPBUSD', 'USDPUSDT']
    bitcoin_trading_fee_promo =['BUSDUSDT', 'TUSDBUSD', 'TUSDUSDT', 'USDCBUSD', 'USDCUSDT', 'USDPBUSD', 'USDPUSDT']
    GCCOUNTER_THRESHOLD=600000
    FIFO = '/looppipe12'
    graph = {}  # grafo
    def main(self):
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
        self.tab = {} #dati
        self.bookdepthdf={} #bookdepth
        coinlist = self.returncoinlist(exchange_info)
        print(len(coinlist))
        time.sleep(5)
        for coin1 in coinlist:
            self.tab[coin1] = {}
            self.bookdepthdf[coin1] = {}
            for coin2 in coinlist:
                self.tab[coin1][coin2] = numpy.NAN
                self.bookdepthdf[coin1][coin2] = numpy.NAN
                pairlist.append(coin1 + '.' + coin2)

        i = 0

        print(len(pairlist))
        bnb_wss_taker = Thread(target=
                               self.threaded_func,
                               args=(pairlist))
        bnb_wss_taker.start()
        pairlist = []
        i = 0
        self.q=posixmq.Queue(self.FIFO)
        Thread(target=self.grapher,args=(self.graph,)).start()
        time.sleep(100)
        print(real_pair_listed)
        self.triangle_calculator( real_pair_listed)


    def returncoinlist(self,exchangeinfo):
        partial_list = []
        for pair in exchangeinfo:
            partial_list.append(pair['baseAsset'])
            partial_list.append(pair['quoteAsset'])
        return list(set(partial_list))

    def triangle_calculator(self,pairlist):
        while True:
            print('graph2',self.graph)
            if not globalgraph.global_graph or globalgraph.global_graph!=self.graph:
                print("[!] Redrawing graph...")
                G = Graph(self.graph)
                labels = dict(zip(G.nodes(), G.nodes()))
                networkx.draw_networkx(G, labels=labels)
                DG = DiGraph(G)
                circular = list(simple_cycles(DG))
                closed_loop_list = [loop for loop in circular if len(loop) == 3]
                globalgraph.global_graph=self.graph
                globalgraph.global_circular=circular
            else:
                closed_loop_list = [loop for loop in globalgraph.global_circular if len(loop) == 3]
            print(f"loops max 3 found{closed_loop_list} length loop {len(closed_loop_list)}")
            #loop_calculator(df,closed_loop_list[0],pairlist,handle)
            for loop in closed_loop_list:
                #Thread(target=loop_calculator,args=(df,loop,pairlist,q,bookdepthdf)).start()
                self.loop_calculator(loop,pairlist)

    def loop_calculator(self,loop,pairlist):
        try:
            """['ETH', 'BTC', 'EUR']  =>  ["ETHBTC", "BTCEUR", "EURETH"]"""
            pairs = [[loop[0],loop[1]],[loop[1],loop[2]],[loop[2],loop[0]]]
            prices=[]
            depths=[]
            margin =0.0
            for pair in pairs:
                if self.isfloat(self.tab[pair[0]][pair[1]]):
                    #print("Testing",pair[0]+pair[1])
                    if pair[0]+pair[1] in pairlist:
                        margin += math.log(float(self.tab[pair[1]][pair[0]]))
                        depths.append(self.bookdepthdf[pair[1]][pair[0]])
                        prices.append(self.tab[pair[1]][pair[0]])
                        if pair[0]+pair[1] in self.zero_trading_fee_promo or pair[1]+pair[0] in self.zero_trading_fee_promo or pair[0]+pair[1] in self.bitcoin_trading_fee_promo or pair[1]+pair[0] in self.bitcoin_trading_fee_promo:
                            margin-= 0
                        else:
                            margin-= 0.00075
                    else:
                        margin += -math.log(float(self.tab[pair[1]][pair[0]]))
                        depths.append(self.bookdepthdf[pair[1]][pair[0]])
                        prices.append(self.tab[pair[1]][pair[0]])
                        if pair[0]+pair[1] in self.zero_trading_fee_promo or pair[1]+pair[0] in self.zero_trading_fee_promo or pair[0]+pair[1] in self.bitcoin_trading_fee_promo or pair[1]+pair[0] in self.bitcoin_trading_fee_promo:
                            margin-= 0
                        else:
                            margin-= 0.00075
            print("Loop %s\t\tMargin %f%%"%(str(loop),margin*100))
            api_message_push = {'loop':pairs,'margin':round(margin*100,5),'prices':prices,'depths':depths,'timestamp':int(datetime.datetime.now().timestamp())}
            if float(api_message_push['margin'])>0:
                with open('timestamplog','a') as f:
                    f.write(f"Timestamp rilevated on websocketgroup: {datetime.datetime.now().timestamp()}\n")
            self.q.put(str(api_message_push))
        except Exception as e:
            with open('culo.txt','a') as f:
                f.write(str(traceback.format_exc()))

    def pair_list_slimmer(self,pair_list, pair):
        new_pair_list = []
        for second_pair in pair_list:
            if pair.split('.')[0] in second_pair or pair.split('.')[1] in second_pair:
                new_pair_list.append(second_pair)
        new_pair_list.remove(pair)
        return new_pair_list

    def isfloat(self,num):
        try:
            float(num)
            return True
        except ValueError:
            return False

    def grapher(self,graph):
        print('Grapher started...')
        while True:
            G=Graph(graph)
            labels = dict(zip(G.nodes(),G.nodes()))
            networkx.draw_networkx(G,labels=labels)
            DG = DiGraph(G)
            plt.show()




    def subscribe_wss(self,api_manager, pairlist):
        stream=[item.lower().replace('.','') for item in pairlist]
        print('sublen %d firstdata %s'%(len(stream),stream[0]))
        api_manager.create_stream(channels=['bookTicker'],markets=stream)


    def threaded_func(self, pairlist):
        print('Starting WSS connection')
        binance_websocket_api_manager = unicorn_binance_websocket_api.BinanceWebSocketApiManager(exchange="binance.com")
        withoutpoint_topoint = dict()
        Thread(target=self.subscribe_wss, args=(binance_websocket_api_manager, pairlist)).start()
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
                        self.tab[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[0]][
                            withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                                1]] = json.loads(res_bnb)['data']['a']
                        self.tab[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[1]][
                            withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                                0]] = json.loads(res_bnb)['data']['b']
                        self.bookdepthdf[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[0]][
                            withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                                1]] = json.loads(res_bnb)['data']['A']
                        self.bookdepthdf[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[1]][
                            withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                                0]] = json.loads(res_bnb)['data']['B']
                        try:
                            if not self.graph[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[0]].__contains__(
                                    withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                                        1]):
                                self.graph[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[0]].append(
                                    withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[
                                        1])
                        except:
                            self.graph[withoutpoint_topoint[json.loads(res_bnb)['data']['s']].split('.')[0]] = []
                else:
                    continue
            except:
                continue


    def log(self,trace, print_flag=False):
        if print_flag:
            print(trace)
        with open('emakerlog', 'a+') as file:
            file.write(
                str(datetime.datetime.now().astimezone()) + " VVVVVVVVVVVVVVVVVVVVVVV\n" + trace + '\n\n')

def go():
    g=globalgraph()
    g.main()
if __name__ == "__main__":
    print('ok')
    #sys.stderr = object
    q1 = queue.Queue()
    go()