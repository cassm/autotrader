from autobahn.asyncio.wamp import ApplicationSession
from autobahn.asyncio.wamp import ApplicationRunner
from autobahn.wamp.types  import SubscribeOptions
from asyncio import coroutine 
import sys
import json
from io import StringIO

poloniex_URL = "wss://api.poloniex.com"    

class TradeClient(ApplicationSession):

    exchange = "NULL"

    def eventProcessor(self, event):
        print("eventProcessor called")
        print("market event received: {}".format(event))

    def onJoin(self, details):
        print("{} client session ready".format(self.exchange))

        def marketEvent(args, kwargs, details):
            print("marketEvent called")#: {}".format(pair))
            
            '''if event.get('type') == 'newTrade':
                eventData = event.get('data')
               ''' 

                #{'data': {'total': '0.00735364', 'date': '2014-11-17 16:34:00', 'tradeID': '748138', 'amount': '5.0125', 'rate': '0.00146706', 'type': 'buy'}, 'type': 'newTrade'}

        # Read in configuration files
        try:
            pairs = [line.strip() for line in open("conf/" + self.exchange + ".conf")]
        except:
            print("Configuration file not found for {}!".format(self.exchange))
            sys.exit(1)

        # Subscribe to each currency pair / topic in the conf file
        for pair in pairs:
            try:
                # provide currency pair name to handler 
                options = SubscribeOptions(details_arg = pair)
                yield from self.subscribe(marketEvent, pair, options)
                print("subscribed to {} on {}".format(pair, self.exchange))
            except Exception as e:
                print("could not subscribe to {} on {}: {}".format(pair, exchange, e))
                sys.exit(1)
        
class PoloniexClient(TradeClient):
    exchange = "poloniex"

poloniex_runner = ApplicationRunner(url = poloniex_URL, realm = "realm1")
poloniex_runner.run(PoloniexClient)
