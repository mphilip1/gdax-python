# gdax/WebsocketClient.py
# original author: Daniel Paquin
# Template object to receive messages from the gdax Websocket Feed

from __future__ import print_function
import json
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException


URL = "wss://ws-feed.gdax.com"


class WebsocketClient(object):
    def __init__(self, products=None, message_type="subscribe", channels=None):
        self.products = products
        self.channels = channels
        self.type = message_type
        self.stop = False
        self.error = None
        self.ws = None
        self.thread = None

    def start(self):
        def _go():
            self._connect()
            self._listen()
            self._disconnect()

        self.stop = False
        self.thread = Thread(target=_go)
        self.thread.start()

    def _connect(self):
        self.products = self.products or ["BTC-USD"]
        self.products = self.products if isinstance(self.products, list) else [self.products]

        sub_params = {'type': 'subscribe', 'product_ids': self.products}
        if self.channels:
            sub_params['channels'] = self.channels

        self.ws = create_connection(URL)
        self.ws.send(json.dumps(sub_params))

        self.on_open()

        heartbeat_on = self.type == "heartbeat"
        sub_params = {"type": "heartbeat", "on": heartbeat_on}
        self.ws.send(json.dumps(sub_params))

    def _listen(self):
        while not self.stop:
            try:
                if int(time.time() % 30) == 0:
                    # Set a 30 second ping to keep connection alive
                    self.ws.ping("keepalive")
                data = self.ws.recv()
                msg = json.loads(data)
            except ValueError as e:
                self.on_error(e)
            except Exception as e:
                self.on_error(e)
            else:
                self.on_message(msg)

    def _disconnect(self):
        if self.type == "heartbeat":
            self.ws.send(json.dumps({"type": "heartbeat", "on": False}))
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass

        self.on_close()

    def close(self):
        self.stop = True
        self.thread.join()

    def on_open(self):
        print("-- Subscribed! --\n")

    def on_close(self):
        print("\n-- Socket Closed --")

    def on_message(self, msg):
        print(msg)

    def on_error(self, e, data=None):
        self.error = e
        print('{} - data: {}'.format(e, data))


if __name__ == "__main__":
    import sys
    import gdax
    import time


    class MyWebsocketClient(gdax.WebsocketClient):
        def on_open(self):
            self.url = "wss://ws-feed.gdax.com/"
            self.products = ["BTC-USD", "ETH-USD"]
            self.message_count = 0
            print("Let's count the messages!")

        def on_message(self, msg):
            print(json.dumps(msg, indent=4, sort_keys=True))
            self.message_count += 1

        def on_close(self):
            print("-- Goodbye! --")


    wsClient = MyWebsocketClient()
    wsClient.start()
    print(wsClient.url, wsClient.products)
    try:
        while True:
            print("\nMessageCount =", "%i \n" % wsClient.message_count)
            time.sleep(1)
    except KeyboardInterrupt:
        wsClient.close()

    if wsClient.error:
        sys.exit(1)
    else:
        sys.exit(0)
