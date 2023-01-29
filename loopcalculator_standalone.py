import datetime
import math
import traceback

import zmq

zero_trading_fee_promo = ['BUSDUSDT', 'TUSDBUSD', 'TUSDUSDT', 'USDCBUSD', 'USDCUSDT', 'USDPBUSD', 'USDPUSDT']
bitcoin_trading_fee_promo = ['BUSDUSDT', 'TUSDBUSD', 'TUSDUSDT', 'USDCBUSD', 'USDCUSDT', 'USDPBUSD', 'USDPUSDT']

def main():
    context = zmq.Context()

    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")
    context1 = zmq.Context()

    socket1 = context1.socket(zmq.REQ)
    socket1.connect("tcp://localhost:5556")
    while True:
        message = socket.recv().decode()
        loop,pairlist,df,bookdepthdf=message.split("|")
        print(loop,pairlist,df,bookdepthdf)
        result = loop_calculator(df,loop,pairlist,bookdepthdf)
        socket1.send(result)
def isfloat(num):
        try:
            float(num)
            return True
        except ValueError:
            return False


def loop_calculator(df, loop, pairlist, bookdepthdf):
    try:
        """['ETH', 'BTC', 'EUR']  =>  ["ETHBTC", "BTCEUR", "EURETH"]"""
        pairs = [[loop[0], loop[1]], [loop[1], loop[2]], [loop[2], loop[0]]]
        prices = []
        depths = []
        margin = 0.0
        for pair in pairs:
            if isfloat(df[pair[0]][pair[1]]):
                #print("Testing", pair[0] + pair[1])
                if pair[0] + pair[1] in pairlist:
                    margin += math.log(float(df[pair[1]][pair[0]]))
                    depths.append(bookdepthdf[pair[1]][pair[0]])
                    prices.append(df[pair[1]][pair[0]])
                    if pair[0] + pair[1] in zero_trading_fee_promo or pair[1] + pair[
                        0] in zero_trading_fee_promo or pair[0] + pair[1] in bitcoin_trading_fee_promo or \
                            pair[1] + pair[0] in bitcoin_trading_fee_promo:
                        margin -= 0
                    else:
                        margin -= 0.00075
                else:
                    margin += -math.log(float(df[pair[1]][pair[0]]))
                    depths.append(bookdepthdf[pair[1]][pair[0]])
                    prices.append(df[pair[1]][pair[0]])
                    if pair[0] + pair[1] in zero_trading_fee_promo or pair[1] + pair[
                        0] in zero_trading_fee_promo or pair[0] + pair[1] in bitcoin_trading_fee_promo or \
                            pair[1] + pair[0] in bitcoin_trading_fee_promo:
                        margin -= 0
                    else:
                        margin -= 0.00075
        #print("Loop %s\t\tMargin %f%%" % (str(loop), margin * 100))
        api_message_push = {'loop': pairs, 'margin': round(margin * 100, 5), 'prices': prices, 'depths': depths,
                            'timestamp': int(datetime.datetime.now().timestamp())}

        return str(api_message_push)
    except Exception as e:
        with open('culo.txt', 'a') as f:
            f.write(str(traceback.format_exc()))

if __name__ == "__main__":
    main()