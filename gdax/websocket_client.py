# gdax/WebsocketClient.py
# original author: Daniel Paquin
# Template object to receive messages from the gdax Websocket Feed

from __future__ import print_function
import json
import base64
import hmac
import hashlib
import time
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException


URL = "wss://ws-feed.gdax.com"
SANDBOX_URL = "wss://ws-feed-public.sandbox.gdax.com"


class WebsocketClient(object):
    def __init__(self, products=None, channels=None, auth=None):
        self.products = products
        self.channels = channels
        self.stop = False
        self.error = None
        self.ws = None
        self.thread = None
        self.auth=auth

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

        if self.auth:
            timestamp = str(time.time())
            message = timestamp + 'GET' + '/users/self/verify'
            message = message.encode('ascii')
            hmac_key = base64.b64decode(self.auth.api_secret)
            signature = hmac.new(hmac_key, message, hashlib.sha256)
            signature_b64 = base64.b64encode(signature.digest())
            sub_params['signature'] = signature_b64
            sub_params['key'] = self.auth.api_key
            sub_params['passphrase'] = self.auth.passphrase
            sub_params['timestamp'] = timestamp

        self.ws = create_connection(URL)
        self.on_open()
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
