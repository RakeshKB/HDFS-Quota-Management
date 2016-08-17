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

class ColoUptimeMetrics(object):
    """
        This class would fetch data from grafana on an hourly basis which
        would return 60 time series data points. Out of which 1 indicates
        the service is up and 0 indicates the service is down
        This data is retrieved based on UTC time zone.

        Data saved to MongoDB is in the form:
        'from_hour': 2,
        'time': 1449027001,
        'to_hour': 3,
        'date': '20160303',
        'data': 100%,
    """

    def __init__(self):
        self.DB_NAME = settings.INFO['MONGO_DB_NAME']
        self.HDFS_UPTIME_COL_NAME = settings.INFO['HDFS_UPTIME_COL_NAME']
        # current hour in UTC format
        self.TO_HOUR = datetime.datetime.utcnow().hour
        self.FROM_HOUR = self.TO_HOUR - 1
        # current date in YYYYMMDD format
        self.CURRENT_DATE = (date.today()-timedelta(0)).strftime("%Y%m%d")
        self.EPOCH_TIME = int(time.time())
        # from_time and to_time should be of the format 'HH:00_YYMMDD' i.e 03:00_20151112
        self.FROM_TIME = "%s:30_%s" % (self.FROM_HOUR, self.CURRENT_DATE)
        self.TO_TIME = "%s:30_%s" % (self.TO_HOUR, self.CURRENT_DATE)
        self.colos = dict(uh1_gold={}, uh1_krypton={}, uj1_topaz={}, lhr1_emerald={}, hkg1_opal={})

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
            self.get_colo_uptime()
        except Exception as e:
            app_logger.debug('reason: %s' % (e))
        try:
            self.update_data_to_mongodb()
        except Exception as e:
            app_logger.debug('could not connect to the mongodb from the ColoUptimeMetrics class, reason: %s' % (e))
            sys.exit(1)

    def get_query(self, colo_name):
        try:
            if colo_name == 'uh1_gold':
                colo_name = 'uh1'
                middle_value = 'UH1_GOLD'
            elif colo_name == 'uh1_krypton':
                colo_name = 'uh1'
                middle_value = 'UH1_Krypton'
            elif colo_name == 'uj1_topaz':
                colo_name = 'uj1'
                middle_value = 'UJ1_Topaz'
            elif colo_name == 'lhr1_emerald':
                colo_name = 'lhr1'
                middle_value = 'LHR1_Emerald'
            elif colo_name == 'hkg1_opal':
                colo_name = 'hkg1'
                middle_value = 'HKG1_Opal'
            else:
                app_logger.info('colo names were not decoded properly')
        except Exception as e:
            app_logger.debug('Reason for not decoding colo names is % s' % (e))

        query = 'http://metrics-web.grid.%s.inmobi.com/render/?&target=\
prod.%s.grid.test.%s.HDFS.uptime&from=%s&until=%s&format=json' % (colo_name, colo_name, middle_value, self.FROM_TIME, self.TO_TIME)
        return query

    def get_data(self, query):
        # print query
        response = requests.get(query)
        # array that saves 1 which indicates that the service was up during that time
        up_array = []
        # array that saves 0 which indicates that the service was down during that time
        down_array = []
        # print response
        if response.status_code == 200:
            try:
                data = json.loads(response.text)
                datapoints = data[0]['datapoints']
                length = len(datapoints)
                if data is not None:
                    for i, j in enumerate(datapoints):
                        if j[0] == 1.0 or j[0] == None:
                            up_array.append(j[0])
                        elif j[0] == 0.0:
                            down_array.append(j[0])
                        else:
                            pass
                else:
                    print "empty data returned"
                    return None
            except Exception as e:
                app_logger.error('Failed decoding JSON: %s' % e)
            up = len(up_array)
            down = len(down_array)
            percentage_uptime = ((float(up-down))/length)*100
            # print percentage_uptime
            return percentage_uptime
        return None

    # def _print(self):
    #     print 'hi'
    #     print '%s' % self.colos['uh1_gold']['middle_value']
    #     print '%s' % self.colos['uh1_krypton']['middle_value']
    #     print '%s' % self.colos['uj1_topaz']['middle_value']
    #     print '%s' % self.colos['lhr1_emerald']['middle_value']
    #     print '%s' % self.colos['hkg1_opal']['middle_value']

    def get_colo_uptime(self):
        for colo in self.colos.keys():
            query = self.get_query(colo)
            result = self.get_data(query)
            if result == None:
                result = 0.0
            # drops data for each colo gets saved in colos[colo]['drops']
            # print colo
            self.colos[colo]['middle_value'] = result
        # self._print()

    def update_data_to_mongodb(self):
        collection = get_connected.connect_to_mongodb(self.HDFS_UPTIME_COL_NAME)
        if collection:
            collection.update({'from_time': self.FROM_TIME}, {'uh1_gold_uptime': self.colos['uh1_gold']['middle_value'], 'uh1_krypton_uptime': self.colos['uh1_krypton']['middle_value'], 'uj1_topaz_uptime': self.colos['uj1_topaz']['middle_value'], 'lhr1_emerald_uptime': self.colos['lhr1_emerald']['middle_value'], 'hkg1_opal_uptime': self.colos['hkg1_opal']['middle_value'], 'from_hour': self.FROM_HOUR, 'to_hour': self.TO_HOUR, 'from_time': self.FROM_TIME, 'epoch_time': self.EPOCH_TIME},  upsert=True)
            collection.find().sort('date', pymongo.ASCENDING)
            app_logger.info('Data inserted successfully')
        else:
            app_logger.debug('empty collection returned to the ColoUptimeMetrics class')
            sys.exit(1)

ColoUptimeMetrics().run()