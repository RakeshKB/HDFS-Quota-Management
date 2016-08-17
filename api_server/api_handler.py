__author__ = 'rakesh.kb'
import sys
import json
import logging
import argparse
# import urllib3
# import certifi
# urllib3.disable_warnings()
from jira.client import JIRA
import webbrowser as wb
from restkit import Resource, BasicAuth, request
import tornado.web
from bson import json_util
from pymongo import MongoClient

from tornado.log import enable_pretty_logging
from api_server import settings
from api_server.db import MongoDBWriter

logger = logging.getLogger('tornado.access')
logger.setLevel(getattr(logging, settings.LOG_LEVEL))
enable_pretty_logging(logger=logger)


def return_success_response(request_handler, reason):
    request_handler.set_header('content-type', 'application/json')
    request_handler.set_header('Access-Control-Allow-Origin', '*')
    message = {'status': 'ok', 'data': reason}
    data = json.dumps(message)
    request_handler.write(data)

def return_error_response(request_handler, reason):
    request_handler.set_header('content-type', 'application/json')
    request_handler.set_header('Access-Control-Allow-Origin', '*')
    message = {'status': 'error', 'data': reason}
    data = json.dumps(message)
    request_handler.write(data)

class Test(tornado.web.RequestHandler):
    def get(self):
        data = {'name': 'rakesh', 'college':'PESIT'}
        try:
            if data:
                return return_success_response(self, reason = '%s' %(data))
        except:
            pass

class HDFSUptimeHandler(tornado.web.RequestHandler):
    """

    """
    def get(self):
        try:
            mongo = MongoDBWriter(collection_type=settings.DB['HDFS_UPTIME_COL_NAME'], logger=logger)
        except Exception as e:
            logger.error('Reason: %s does not exist' %(e))
            return return_error_response(self, reason = '%s does not exist' %(e))
        try:
            mongo._connect_to_mongodb()
        except Exception as e:
            logger.error('Reason: %s' %(e))
            return return_error_response(self, reason = '%s' %(e))
        try:
            # limit is 12 in order to get the hourly usage for last 12 hours
            cursor = mongo.collection.find({}, {"_id": 0}).sort("epoch_time", -1).limit(1)
            result = []
            for index in cursor:
                result.append(index)

            if result:
                logger.debug('data returned for the class HDFSUptimeHandler is: %s' % (result))
                return return_success_response(self, reason=result)
            else:
                logger.debug('no json data returned for the class HDFSUptimeHandler')
                return return_success_response(self, reason='no json data returned for the class HDFSUptimeHandler')

        except Exception as e:
            logger.error('Reason : %s' % (e))
            return return_error_response(self, reason='%s' % (e))


class FileCountsHandler(tornado.web.RequestHandler):
    """

    """
    def get(self):
        try:
            mongo = MongoDBWriter(collection_type=settings.DB['HDFS_FILE_COUNTS_COL_NAME'], logger=logger)
        except Exception as e:
            logger.error('Reason: %s does not exist' %(e))
            return return_error_response(self, reason = '%s does not exist' %(e))
        try:
            mongo._connect_to_mongodb()
        except Exception as e:
            logger.error('Reason: %s' %(e))
            return return_error_response(self, reason = '%s' %(e))
        try:
            # limit is 12 in order to get the hourly usage for last 12 hours
            cursor = mongo.collection.find({}, {"_id": 0}).sort("from_time", -1).limit(1)
            result = []
            for index in cursor:
                result.append(index)

            if result:
                logger.debug('data returned for the class FileCountsHandler is: %s' % (result))
                return return_success_response(self, reason=result)
            else:
                logger.debug('no json data returned for the class FileCountsHandler')
                return return_success_response(self, reason='no json data returned for the class FileCountsHandler')

        except Exception as e:
            logger.error('Reason : %s' % (e))
            return return_error_response(self, reason='%s' % (e))

class UserPlatformHandler(tornado.web.RequestHandler):
    """

    """
    def get(self):
        try:
            mongo = MongoDBWriter(collection_type=settings.DB['USER_PLATFORM_COL_NAME'], logger=logger)
        except Exception as e:
            logger.error('Reason: %s does not exist' %(e))
            return return_error_response(self, reason = '%s does not exist' %(e))
        try:
            mongo._connect_to_mongodb()
        except Exception as e:
            logger.error('Reason: %s' %(e))
            return return_error_response(self, reason = '%s' %(e))
        try:
            # limit is 12 in order to get the hourly usage for last 12 hours
            cursor = mongo.collection.find({}, {"_id": 0}).sort("epoch_time", -1).limit(6)
            result = []
            for index in cursor:
                result.append(index)

            if result:
                logger.debug('data returned for the class UserPlatformHandler is: %s' % (result))
                return return_success_response(self, reason=result)
            else:
                logger.debug('no json data returned for the class UserPlatformHandler')
                return return_success_response(self, reason='no json data returned for the class UserPlatformHandler')

        except Exception as e:
            logger.error('Reason : %s' % (e))
            return return_error_response(self, reason='%s' % (e))

class UserYodaHandler(tornado.web.RequestHandler):
    """

    """
    def get(self):
        try:
            mongo = MongoDBWriter(collection_type=settings.DB['USER_YODA_COL_NAME'], logger=logger)
        except Exception as e:
            logger.error('Reason: %s does not exist' %(e))
            return return_error_response(self, reason = '%s does not exist' %(e))
        try:
            mongo._connect_to_mongodb()
            print "here"
        except Exception as e:
            logger.error('Reason: %s' %(e))
            return return_error_response(self, reason = '%s' %(e))
        try:
            # limit is 12 in order to get the hourly usage for last 12 hours
            cursor = mongo.collection.find({}, {"_id": 0}).sort("epoch_time", -1).limit(6)
            result = []
            for index in cursor:
                result.append(index)

            if result:
                logger.debug('data returned for the class UserYodaHandler is: %s' % (result))
                return return_success_response(self, reason=result)
            else:
                logger.debug('no json data returned for the class UserYodaHandler')
                return return_success_response(self, reason='no json data returned for the class UserYodaHandler')

        except Exception as e:
            logger.error('Reason : %s' % (e))
            return return_error_response(self, reason='%s' % (e))

class UserYodaWarehouseHandler(tornado.web.RequestHandler):
    """

    """
    def get(self):
        try:
            mongo = MongoDBWriter(collection_type=settings.DB['USER_YODA_WAREHOUSE_COL_NAME'], logger=logger)
        except Exception as e:
            logger.error('Reason: %s does not exist' %(e))
            return return_error_response(self, reason = '%s does not exist' %(e))
        try:
            mongo._connect_to_mongodb()
            print "here"
        except Exception as e:
            logger.error('Reason: %s' %(e))
            return return_error_response(self, reason = '%s' %(e))
        try:
            # limit is 12 in order to get the hourly usage for last 12 hours
            cursor = mongo.collection.find({}, {"_id": 0}).sort("epoch_time", -1).limit(6)
            result = []
            for index in cursor:
                result.append(index)

            if result:
                logger.debug('data returned for the class UserYodaWarehouseHandler is: %s' % (result))
                return return_success_response(self, reason=result)
            else:
                logger.debug('no json data returned for the class UserYodaWarehouseHandler')
                return return_success_response(self, reason='no json data returned for the class UserYodaWarehouseHandler')

        except Exception as e:
            logger.error('Reason : %s' % (e))
            return return_error_response(self, reason='%s' % (e))

class HDFSUsageHandler(tornado.web.RequestHandler):
    """

    """
    def get(self):
        try:
            mongo = MongoDBWriter(collection_type=settings.DB['HDFS_USAGE_COL_NAME'], logger=logger)
        except Exception as e:
            logger.error('Reason: %s does not exist' %(e))
            return return_error_response(self, reason = '%s does not exist' %(e))
        try:
            mongo._connect_to_mongodb()
            print "here"
        except Exception as e:
            logger.error('Reason: %s' %(e))
            return return_error_response(self, reason = '%s' %(e))
        try:
            # limit is 12 in order to get the hourly usage for last 12 hours
            cursor = mongo.collection.find({}, {"_id": 0}).sort("from_time", -1).limit(12)
            result = []
            for index in cursor:
                result.append(index)

            if result:
                logger.debug('data returned for the class HDFSUsageHandler is: %s' % (result))
                return return_success_response(self, reason=result)
            else:
                logger.debug('no json data returned for the class HDFSUsageHandler')
                return return_success_response(self, reason='no json data returned for the class HDFSUsageHandler')

        except Exception as e:
            logger.error('Reason : %s' % (e))
            return return_error_response(self, reason='%s' % (e))

class TestDataHandler(tornado.web.RequestHandler):
    """
        This class is written as a part of testing the MVC architecture
        The data is polled from the db in the backend.
        This data has to be sent by the server as a request from the client
    """
    def get(self):
        try:
            mongo = MongoDBWriter(collection_type=settings.DB['MONGO_COL_TEST_DATA'], logger=logger)
        except Exception as e:
            logger.error('Reason: %s does not exist' %(e))
            return return_error_response(self, reason = '%s does not exist' %(e))
        try:
            mongo._connect_to_mongodb()
        except Exception as e:
            logger.error('Reason: %s' %(e))
            return return_error_response(self, reason = '%s' %(e))
        try:
            cursor = mongo.collection.find({}, {'id':0}).sort('from_time', -1).limit(1)
            result = []
            for index in cursor:
                result.append(index)
            if result:
                logger.debug('data returned from the TestDataHandler class is %s' % (result))
                return return_success_response(self, reason=result)
            else:
                logger.debug('data not returned from the TestDataHandler class')
                return return_error_response(self, reason = '%s' %(e))
        except Exception as e:
            logger.error('Reason : %s' % (e))
            return return_error_response(self, reason='%s' % (e))

class EditJiraHandler(tornado.web.RequestHandler):
    """
        Editing an existing jira issue
    """
    def post(self):
        self.USER_NAME = self.get_argument('user_name', '')
        self.MANAGER_NAME = self.get_argument('manager_name', '')
        self.PROJECT_DESCRIPTION = self.get_argument('project_desc', '')
        self.PRODUCT_DESCRIPTION = self.get_argument('product_desc', '')
        self.REPLICATION_NEEDED = self.get_argument('replication_needed', '')
        self.PROJECT_SLASHNAME = self.get_argument('dir_path', '')
        self.CAPACITY_REQUIREMENTS = self.get_argument('capacity_reqs', '')
        self.HEADLESS_ACCOUNT = self.get_argument('headless_account', '')
        self.JIRA_ISSUE = self.get_argument('jira_ticket', '')
        
        self.SERVER_URL = settings.DB['JIRA_ACCESS_LINK']
        self.JIRA_USERNAME = settings.DB['JIRA_USERNAME']
        self.OPTIONS = settings.DB['JIRA_OPTIONS']
        self.JIRA_PASSWORD = settings.DB['JIRA_PASSWORD']

        print self.JIRA_ISSUE


class CreateJiraHandler(tornado.web.RequestHandler):
    """
        Creating a new jira issue
    """
    
    def post(self):
        self.USER_NAME = self.get_argument('user_name', '')
        self.MANAGER_NAME = self.get_argument('manager_name', '')
        self.PROJECT_DESCRIPTION = self.get_argument('project_desc', '')
        self.PRODUCT_DESCRIPTION = self.get_argument('product_desc', '')
        self.REPLICATION_NEEDED = self.get_argument('replication_needed', '')
        self.PROJECT_SLASHNAME = self.get_argument('dir_path', '')
        self.CAPACITY_REQUIREMENTS = self.get_argument('capacity_reqs', '')
        self.HEADLESS_ACCOUNT = self.get_argument('headless_account', '')
        self.SERVER_URL = settings.DB['JIRA_ACCESS_LINK']
        self.JIRA_USERNAME = settings.DB['JIRA_USERNAME']
        self.OPTIONS = settings.DB['JIRA_OPTIONS']
        self.JIRA_PASSWORD = settings.DB['JIRA_PASSWORD']
        
        try:
            jira = JIRA(self.OPTIONS, basic_auth=(self.JIRA_USERNAME, self.JIRA_PASSWORD))
            print jira
            # issue_dict = {}
            new_issue = jira.create_issue(project={'key': ""+self.PROJECT_DESCRIPTION+""}, summary=""+self.PROJECT_DESCRIPTION+"", description=""+self.PRODUCT_DESCRIPTION+"", assignee={'name': self.USER_NAME, 'emailAddress': self.USER_NAME+'@inmobi.com'}, issuetype={'name': 'Task'}, components=[{'name': 'Grid-Infra'},],)
            # new_issue = jira.create_issue(project={'key': self.PROJECT_DESCRIPTION}, description= self.PRODUCT_DESCRIPTION, assignee={'name': self.USER_NAME, 'email': self.USER_NAME+'@inmobi.com'}, isseutype={'name': 'Task'}, components={'name': 'GRID-Infra'})
            print 'issue %s created' % new_issue

        except Exception as e:
            logger.error('Reason: %s' % (e))
            return return_error_response(self, reason='%s' % (e))

