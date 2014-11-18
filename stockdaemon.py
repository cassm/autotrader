#!/usr/bin/env python3
from autobahn.asyncio.wamp import ApplicationSession
from autobahn.asyncio.wamp import ApplicationRunner
from autobahn.wamp.types  import SubscribeOptions
from tzlocal import get_localzone 
from pymongo import MongoClient    
from asyncio import coroutine
from dateutil import parser
from time import strftime
import logging
import sys

class TradeClient(ApplicationSession):
    exchange = "NULL"

    def eventProcessor(self, event):
        logger.debug("eventProcessor called")
        logger.debug("market event received: {}".format(event))

    def onJoin(self, details):
        logger.info("{} client session ready".format(self.exchange))

        def marketEvent(event, **details):
            if event.get('type') == 'newTrade':
                eventData = event.get('data')

                # create collection name in format exchange_CURR1_CURR2
                collectionName = self.exchange + "_" + list(details)[0]

                # Create sensible timestamp
                timezone = get_localzone()
                timestr = eventData.get('date')
                eventTime = timezone.localize(parser.parse(timestr))

                ''' N.B. Poloniex newTrade event data format is as follows:

                Example: BTC_XMR. Buy of 5.0125 XMR at 0.00146706 BTC each, totalling 
                         0.00735364 BTC.

                {'total': '0.00735364', 
                'date': '2014-11-17 16:34:00', 
                'tradeID': '748138', 
                'amount': '5.0125', 
                'rate': '0.00146706', 
                'type': 'buy'} '''

                # construct entry
                entry = {'amount': eventData.get('amount'), 
                         'rate': eventData.get('rate'),
                         'date': eventTime}
                
                # insert into appropriate collection
                try:
                    objectID = eval('db.' + collectionName + '.insert(entry)')
                except(e):
                    logger.info("Failed to insert entry into {}: {}".format(collectionName, e))
                result = eval('db.' + collectionName + '.find_one({\'_id\': objectID})')
                logger.debug(str(result))


        # Read in configuration files
        try:
            pairs = [line.strip() for line in open("conf/" + self.exchange + ".conf")]
        except:
            logger.info("Configuration file not found for {}!".format(self.exchange))
            sys.exit(1)

        # Subscribe to each currency pair / topic in the conf file
        for pair in pairs:
            try:
                # provide currency pair name to handler 
                options = SubscribeOptions(details_arg = pair)
                yield from self.subscribe(marketEvent, pair, options)
                logger.info("subscribed to {} on {}".format(pair, self.exchange))

                # create and store collection
                collectionName = self.exchange + "_" + pair
                                                                   
                collection = mongoClient[collectionName]
                
            except Exception as e:
                logger.info("could not subscribe to {} on {}: {}".format(pair, exchange, e))
                sys.exit(1)
        
class PoloniexClient(TradeClient):
    exchange = "poloniex"

# set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.CRITICAL)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('stockdaemon.log')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# initialise mongo client & open database
mongoClient = MongoClient()
db = mongoClient['autotrader']

# run poloniex WAMP client
poloniex_URL = "wss://api.poloniex.com"    
poloniex_runner = ApplicationRunner(url = poloniex_URL, realm = "realm1")
poloniex_runner.run(PoloniexClient)
