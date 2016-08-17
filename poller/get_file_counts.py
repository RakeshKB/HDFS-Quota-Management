import sys
import json
import time
import pymongo
import datetime
import requests
from datetime import date, timedelta
from pymongo import MongoClient
from poller.log import setup_logger
import poller.connect_to_mongo_db
from poller import settings

get_connected = poller.connect_to_mongo_db.ConnectToMongoDb()
app_logger = setup_logger()

class FileCountMetrics(object):
    """
        This class would fetch the total number of files that are present in the
        directories like (data, user, project and projects)
        Data saved to MongoDB is in the form:
        'from_hour': 2,
        'time': 1449027001,
        'to_hour': 3,
        'date': '20160303',
    """
    def __init__(self):
        self.DB_NAME = settings.INFO['MONGO_DB_NAME']
        self.HDFS_FILE_COUNTS_COL_NAME = settings.INFO['HDFS_FILE_COUNTS_COL_NAME']
        # current hour in UTC format
        self.TO_HOUR = datetime.datetime.utcnow().hour
        self.FROM_HOUR = self.TO_HOUR - 1
        # current date in YYYYMMDD format
        self.CURRENT_DATE = (date.today()-timedelta(0)).strftime("%Y%m%d")
        self.EPOCH_TIME = int(time.time())
        # from_time and to_time should be of the format 'HH:00_YYMMDD' i.e 03:00_20151112
        self.FROM_TIME = "%s:30_%s" % (self.FROM_HOUR, self.CURRENT_DATE)
        self.TO_TIME = "%s:30_%s" % (self.TO_HOUR, self.CURRENT_DATE)
        self.DIRECTORY = dict(data={}, user={}, project={}, projects={})

    def run(self):
        """
            The hdfs usage details are queried on UTC time stamp
            for example: query from 05:00 AM to 06:00 AM in UTC
            is 10:30 AM to 11:30 AM in IST. Hence, we query from
            05:30 AM to 06:30 AM in UTC in order to get the data
            from 11:00 AM to 12:00 AM in IST
            This will help in clear display of data in the client 
            side
        """
        try:
            self.get_file_count_details()
        except Exception as e:
            app_logger.debug('reason: %s' % (e))
        try:
            self.update_data_to_mongodb()
        except Exception as e:
            app_logger.debug('could not connect to the mongodb from the HDFSUsageMetrics class, reason: %s' % (e))
            sys.exit(1)

    def get_data(self, query):
        response = requests.get(query)
        count = 1
        if response.status_code == 200:
            try:
                data = json.loads(response.text)
                length = len(data[0]['datapoints'])
                data = data[0]['datapoints'][length-1][0]
                while data is None:
                    data = json.loads(response.text)
                    length = int(len(data[0]['datapoints'])-count)
                    count+=1
                    data = data[0]['datapoints'][length][0]
                if data is not None:
                    return data
                else:
                    print "empty data returned"
                    return None
            except Exception as e:
                app_logger.error('Failed decoding JSON: %s' % e)
        return None

    def get_file_count_query(self, directory):
        hdfs_file_count_query = "groupByNode(prod.uh1.grid.UH1_GOLD.app.glgw4009.quota.%s.*.FILE_COUNT, 7, 'sum')&format=json" %(directory)
        query = settings.INFO['HDFS_USAGE_METRICS_BASE_QUERY'] + hdfs_file_count_query
        return query 
        
    def get_file_count_details(self):
        for directory in self.DIRECTORY.keys():
            hdfs_file_count_query = self.get_file_count_query(directory)
            result = self.get_data(hdfs_file_count_query)
            if result is not None:
                self.DIRECTORY[directory]['file_count'] = result
            else:
                app_logger.debug('No data returned')
                self.DIRECTORY[directory]['data'] = 0

    def update_data_to_mongodb(self):
        """
            Connection to mongo db via connect_to_mongodb with the collection_name
        """
        collection_name = self.HDFS_FILE_COUNTS_COL_NAME
        collection = get_connected.connect_to_mongodb(collection_name)
        if collection:
            collection.update({'from_time': self.FROM_TIME}, {'file_usage_data': self.DIRECTORY, 'from_hour': self.FROM_HOUR, 'to_hour': self.TO_HOUR, 'from_time': self.FROM_TIME, 'epoch_time': self.EPOCH_TIME }, upsert=True)
            collection.find().sort('date', pymongo.ASCENDING)
            app_logger.info('Data inserted ')
        else:
            app_logger.debug('empty collection returned to the Ad_Metrics class')
            sys.exit(1)

FileCountMetrics().run()
















