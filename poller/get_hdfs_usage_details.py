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

class HDFSUsageMetrics(object):
    """
        This class would fetch data from grafana on an hourly basis summarizing the
        datapoints for each hour. This data is retrieved based on UTC time zone.

        Data saved to MongoDB is in the form:
        'from_hour': 2,
        'time': 1449027001,
        'to_hour': 3,
        'date': '20160303',
        'data': 5469642301960839.0,
        'type' : 'PiB'
    """

    def __init__(self):
        self.DB_NAME = settings.INFO['MONGO_DB_NAME']
        self.HDFS_USAGE_COL_NAME = settings.INFO['HDFS_USAGE_COL_NAME']
        # current hour in UTC format
        self.TO_HOUR = datetime.datetime.utcnow().hour
        self.FROM_HOUR = self.TO_HOUR - 1
        # current date in YYYYMMDD format
        self.CURRENT_DATE = (date.today()-timedelta(0)).strftime("%Y%m%d")
        self.EPOCH_TIME = int(time.time())
        # from_time and to_time should be of the format 'HH:00_YYMMDD' i.e 03:00_20151112
        self.FROM_TIME = "%s:30_%s" % (self.FROM_HOUR, self.CURRENT_DATE)
        self.TO_TIME = "%s:30_%s" % (self.TO_HOUR, self.CURRENT_DATE)

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
            self.CAPACITY_TOTAL, self.CAPACITY_USED = self.get_hdfs_usage_details()
        except Exception as e:
            app_logger.debug('reason: %s' % (e))
        try:
            self.update_data_to_mongodb(self.CAPACITY_TOTAL, self.CAPACITY_USED)
        except Exception as e:
            app_logger.debug('could not connect to the mongodb from the HDFSUsageMetrics class, reason: %s' % (e))
            sys.exit(1)

    def get_hdfs_data(self, query):
        #get response code
        response = requests.get(query)
        if response.status_code == 200:
            try:
                data = json.loads(response.text)
                data = data[0]['datapoints'][0][0]
                if data is not None:
                    return data
                else:
                    print "empty data returned"
                    return None
            except Exception as e:
                app_logger.error('Failed decoding JSON: %s' % e)
        return None

    def get_hdfs_capacity_total_query(self):
        hdfs_capacity_total_query = "summarize(prod.uh1.grid.UH1_GOLD.app.glgm1001.dfs.FSNamesystem.\
CapacityTotal,'1hour','avg',true)&from=%s&until=%s&format=json" % (self.FROM_TIME, self.TO_TIME)
        query = settings.INFO['HDFS_USAGE_METRICS_BASE_QUERY'] + hdfs_capacity_total_query
        return query

    def get_hdfs_capacity_used_query(self):
        hdfs_capacity_used_query = "summarize(prod.uh1.grid.UH1_GOLD.app.glgm1001.dfs.FSNamesystem.\
CapacityUsed,'1hour','avg',true)&from=%s&until=%s&format=json" % (self.FROM_TIME, self.TO_TIME)
        query = settings.INFO['HDFS_USAGE_METRICS_BASE_QUERY'] + hdfs_capacity_used_query
        return query

    def get_hdfs_usage_details(self):
        hdfs_capacity_total_query = self.get_hdfs_capacity_total_query()
        hdfs_capacity_total_result = self.get_hdfs_data(hdfs_capacity_total_query)
        if hdfs_capacity_total_result == None:
            hdfs_capacity_total_result = 0.0
        else:
            hdfs_capacity_total_result = round((hdfs_capacity_total_result/(1024**5)),2)

        hdfs_capacity_used_query = self.get_hdfs_capacity_used_query()
        hdfs_capacity_used_result = self.get_hdfs_data(hdfs_capacity_used_query)
        if hdfs_capacity_used_result == None:
            hdfs_capacity_used_result = 0.0
        else:
            hdfs_capacity_used_result = round((hdfs_capacity_used_result/(1024**5)),2)
        print "Total Capacity = %s PiB\nCapacity Used = %s PiB" % (hdfs_capacity_total_result, hdfs_capacity_used_result)
        return hdfs_capacity_total_result, hdfs_capacity_used_result

    def update_data_to_mongodb(self, capacity_total, capacity_used):
        """
            Connection to mongo db via connect_to_mongodb with the collection_name
        """
        collection_name = self.HDFS_USAGE_COL_NAME
        collection = get_connected.connect_to_mongodb(collection_name)
        if collection:
            collection.update({'from_time': self.FROM_TIME}, {'data_in_PiB':{'capacity_total': capacity_total, 'capacity_used': capacity_used}, 'from_hour': self.FROM_HOUR, 'to_hour': self.TO_HOUR, 'from_time': self.FROM_TIME, 'epoch_time': self.EPOCH_TIME }, upsert=True)
            collection.find().sort('date', pymongo.ASCENDING)
            app_logger.info('Data inserted successfully')
        else:
            app_logger.debug('empty collection returned to the Ad_Metrics class')
            sys.exit(1)

HDFSUsageMetrics().run()




