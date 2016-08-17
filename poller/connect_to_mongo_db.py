import logging
from pymongo import MongoClient
from poller import settings
import poller.log
logger = poller.log.setup_logger()


class ConnectToMongoDb(object):
    def __init__(self):
        logger.info('Initialising variables of ConnectToMongoDb class')
        self.DB_NAME = settings.INFO['MONGO_DB_NAME']
        logger.info('Initialisation completed')

    def connect_to_mongodb(self, collection_name):
        try:
            connected = MongoClient('localhost', 27017)
            db = connected[self.DB_NAME]
            collection = db[collection_name]
            logger.info('Mongo DB connection successful')
            return collection
        except Exception as e:
            logger.debug('reason: %s' % (e))
            return None
