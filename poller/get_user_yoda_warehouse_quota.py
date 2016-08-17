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

class UserYodaWarehouseMetrics(object):
    """
        This class would fetch data from grafana on an hourly basis
        This data is retrieved based on UTC time zone.

        Data saved to MongoDB is in the form:
        'date': '20160303',
        'data': 5469642301960839.0,
        'type' : 'TiB'
    """

    def __init__(self):
        self.DB_NAME = settings.INFO['MONGO_DB_NAME']
        self.USER_YODA_WAREHOUSE_COL_NAME = settings.INFO['USER_YODA_WAREHOUSE_COL_NAME']
        # current hour in UTC format
        # self.TO_HOUR = datetime.datetime.utcnow().hour
        # self.FROM_HOUR = self.TO_HOUR - 1
        # Getting the quota usage details for a day, hence tme is hardcoded from 00:00 to 23:59
        self.FROM_HOUR = 00
        self.TO_HOUR = 23
        self.DAY = 1
        # current date in YYYYMMDD format
        self.CURRENT_DATE = (date.today()-timedelta(self.DAY)).strftime("%Y%m%d")
        self.EPOCH_TIME = int(time.time())
        # from_time and to_time should be of the format 'HH:00_YYMMDD' i.e 03:00_20151112
        self.FROM_TIME = "%s:00_%s" % (self.FROM_HOUR, self.CURRENT_DATE)
        self.TO_TIME = "%s:59_%s" % (self.TO_HOUR, self.CURRENT_DATE)

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
            self.TOTAL_ALLOCATED_SPACE , self.TOTAL_USED_SPACE, self.TOTAL_REMAINING_SPACE = self.get_user_yoda_warehouse_details()
        except Exception as e:
            app_logger.debug('reason: %s' % (e))
        try:
            self.update_data_to_mongodb(self.TOTAL_ALLOCATED_SPACE , self.TOTAL_USED_SPACE, self.TOTAL_REMAINING_SPACE)
        except Exception as e:
            app_logger.debug('could not connect to the mongodb from the HDFSUsageMetrics class, reason: %s' % (e))
            sys.exit(1)

    def get_data(self, query):
        #get response code
        response = requests.get(query)
        if response.status_code == 200:
            try:
                data = json.loads(response.text)
                length = len(data[0]['datapoints'])
                data = data[0]['datapoints'][length-1][0]
                if data is None:
                    print "empty data returned"
                    return None
                else:
                    return data
            except Exception as e:
                app_logger.error('Failed decoding JSON: %s' % e)
        return None

    def get_space_quota_query(self):
        space_quota_query = "scale(prod.uh1.grid.UH1_GOLD.app.glgw4009.quota.user.yoda.warehouse.usercube.\
SPACE_QUOTA, 1)\&from=%s&until=%s&format=json" % (self.FROM_TIME, self.TO_TIME)
        query = settings.INFO['HDFS_USAGE_METRICS_BASE_QUERY'] + space_quota_query
        return query

    def get_remaining_space_quota_query(self):
        remaining_space_quota_query = "scale(prod.uh1.grid.UH1_GOLD.app.glgw4009.quota.user.yoda.warehouse.usercube.\
REMAINING_SPACE_QUOTA, 1)\&from=%s&until=%s&format=json" % (self.FROM_TIME, self.TO_TIME)
        query = settings.INFO['HDFS_USAGE_METRICS_BASE_QUERY'] + remaining_space_quota_query
        return query

    def get_user_yoda_warehouse_details(self):
        query = self.get_space_quota_query()
        self.TOTAL_ALLOCATED_SPACE = self.get_data(query)

        query = self.get_remaining_space_quota_query()
        self.TOTAL_REMAINING_SPACE = self.get_data(query)

        if self.TOTAL_ALLOCATED_SPACE  == None:
            self.TOTAL_ALLOCATED_SPACE  = 0.0
        else:
            self.TOTAL_ALLOCATED_SPACE  = round((self.TOTAL_ALLOCATED_SPACE /(1024**4)),2)

        if self.TOTAL_REMAINING_SPACE == None:
            self.TOTAL_REMAINING_SPACE = 0.0
        else:
            self.TOTAL_REMAINING_SPACE = round((self.TOTAL_REMAINING_SPACE /(1024**4)),2)

        self.TOTAL_USED_SPACE = round((self.TOTAL_ALLOCATED_SPACE  - self.TOTAL_REMAINING_SPACE),2)

        print "Total Allocated Space is %s TiB" % (self.TOTAL_ALLOCATED_SPACE)
        print "Total Used Space is %s TiB" % (self.TOTAL_USED_SPACE)
        print "Total Remaining Space is %s TiB" % (self.TOTAL_REMAINING_SPACE)


        return self.TOTAL_ALLOCATED_SPACE , self.TOTAL_USED_SPACE, self.TOTAL_REMAINING_SPACE

    def update_data_to_mongodb(self, total_allocated_space, total_used_space, total_remaining_space):
        """
            Connection to mongo db via connect_to_mongodb with the collection_name
        """
        collection_name = self.USER_YODA_WAREHOUSE_COL_NAME
        collection = get_connected.connect_to_mongodb(collection_name)
        if collection:
            collection.update({'date': self.CURRENT_DATE}, {'date': self.CURRENT_DATE, 'total_allocated_space_TiB':total_allocated_space, 'total_used_space_TiB': total_used_space, 'total_remaining_space_TiB':total_remaining_space, 'epoch_time': self.EPOCH_TIME }, upsert=True)
            collection.find().sort('date', pymongo.ASCENDING)
            app_logger.info('Data inserted successfully')
        else:
            app_logger.debug('empty collection returned to the Ad_Metrics class')
            sys.exit(1)

UserYodaWarehouseMetrics().run()




