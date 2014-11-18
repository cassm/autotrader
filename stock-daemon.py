from autobahn.asyncio.wamp import ApplicationSession
from autobahn.asyncio.wamp import ApplicationRunner
from autobahn.wamp.types  import SubscribeOptions
from tzlocal import get_localzone 
from pymongo import MongoClient    
from asyncio import coroutine
from dateutil import parser
import sys
        
mongoClient = MongoClient()
db = mongoClient['autotrader']

poloniex_URL = "wss://api.poloniex.com"    


class TradeClient(ApplicationSession):

    exchange = "NULL"

    def eventProcessor(self, event):
        print("eventProcessor called")
        print("market event received: {}".format(event))

    def onJoin(self, details):
        print("{} client session ready".format(self.exchange))

        def marketEvent(event, **details):
            if event.get('type') == 'newTrade':
                print(event.get('type'))

                eventData = event.get('data')

                # create collection name in format exchange_CURR1_CURR2
                collectionName = self.exchange + "_" + list(details)[0]

                # Create sensible timestamp
                timezone = get_localzone()
                timestr = eventData.get('date')
                eventTime = timezone.localize(parser.parse(timestr))

                # construct entry

                '''
                N.B. Poloniex newTrade event data format is as follows:

                Example: BTC_XMR. Buy of 5.0125 XMR at 0.00146706 BTC each, totalling 
                         0.00735364 BTC.

                {'total': '0.00735364', 
                'date': '2014-11-17 16:34:00', 
                'tradeID': '748138', 
                'amount': '5.0125', 
                'rate': '0.00146706', 
                'type': 'buy'}
                '''

                entry = {'amount': eventData.get('amount'), 
                         'rate': eventData.get('rate'),
                         'date': eventTime}
                
                # insert into appropriate collection
                try:
                    objectID = eval('db.' + collectionName + '.insert(entry)')
                except(e):
                    print("Could not insert entry into {}: {}".format(collectionName, e))
                result = eval('db.' + collectionName + '.find_one({\'_id\': objectID})')
                print(str(result))


        # Read in configuration files
        try:
            pairs = [line.strip() for line in open("conf/" + self.exchange + ".conf")]
        except:
            print("Configuration file not found for {}!".format(self.exchange))
            sys.exit(1)

        testColl = mongoClient['test']

        # Subscribe to each currency pair / topic in the conf file
        for pair in pairs:
            try:
                # provide currency pair name to handler 
                options = SubscribeOptions(details_arg = pair)
                yield from self.subscribe(marketEvent, pair, options)
                print("subscribed to {} on {}".format(pair, self.exchange))

                # create and store collection
                collectionName = self.exchange + "_" + pair
                try:
                    collection = mongoClient[collectionName]
                    print("MongoDB collection \"{}\" opened.".format(collectionName))
                except(e):
                    print("could not open collection for {}!".format(collectionName))

            except Exception as e:
                print("could not subscribe to {} on {}: {}".format(pair, exchange, e))
                sys.exit(1)
        
class PoloniexClient(TradeClient):
    exchange = "poloniex"

poloniex_runner = ApplicationRunner(url = poloniex_URL, realm = "realm1")
poloniex_runner.run(PoloniexClient)
