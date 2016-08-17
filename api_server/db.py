from __future__ import absolute_import
import json
import logging
import pymongo
from api_server import settings

__author__ = 'rakesh.kb'


class MongoDBWriter(object):

    def __init__(self, collection_type, logger):
        """
        Helper class to write
        deployment logs and status
        into their respective collections.
        :param logger logger:
        :return: Boolean
        """

        self._logger = logger
        self._host = settings.DB['MONGO_HOST']
        self._port = settings.DB['MONGO_PORT']
        self._connection = ''
        self._db = ''
        self._collection_type = collection_type
        self.collection = None

    def _connect_to_mongodb(self):
        """
            connects to MongoDB to
            their respective collections
            as mentioned in ``settings.py``
            :return: Boolean
        """

        try:
            self._connection = pymongo.MongoClient(self._host, self._port)
        except Exception as e:
            self._logger.error('Unable to connect to the Mongo Host : %s with Port : %s ' % (self._host, self._port))
            raise e

        try:
            self.db = getattr(self._connection, settings.DB['MONGO_DB_NAME'])
        except AttributeError as e:
            self._logger.error('Unable to select the DB : %s, Reason : %s' % (settings.DB['MONGO_DB_NAME'], e))
            raise e

        try:
            if self._collection_type == 'hdfs_test_data':
                self.collection = getattr(self.db, settings.DB['MONGO_COL_TEST_DATA'])
            elif self._collection_type == 'hdfs_usage_data':
                self.collection = getattr(self.db, settings.DB['HDFS_USAGE_COL_NAME'])
            elif self._collection_type == 'hdfs_file_counts_data':
                self.collection = getattr(self.db, settings.DB['HDFS_FILE_COUNTS_COL_NAME'])
            elif self._collection_type == 'userplatform_quota_usage':
                self.collection = getattr(self.db, settings.DB['USER_PLATFORM_COL_NAME'])
            elif self._collection_type == 'user_yoda_quota_usage':
                self.collection = getattr(self.db, settings.DB['USER_YODA_COL_NAME'])
            elif self._collection_type == 'user_yoda_warehouse_quota_usage':
                self.collection = getattr(self.db, settings.DB['USER_YODA_WAREHOUSE_COL_NAME'])
            elif self._collection_type == 'hdfs_uptime_data':
                self.collection = getattr(self.db, settings.DB['HDFS_UPTIME_COL_NAME'])
            else:
                raise ValueError('Invalid collection : %s' % self._collection_type)

        except AttributeError as e:
            self._logger.error('Unable to select the collection reason : %s' % e)
            raise e

