#!/usr/bin/python
# -*- coding: utf-8 -*-
# This Python file uses the following encoding: utf-8
LOG_LEVEL = 'DEBUG'

DB = dict(
    MONGO_HOST='localhost',
    MONGO_PORT=27017,
    MONGO_DB_NAME='hdfs_quota_management',
    MONGO_COL_TEST_DATA='hdfs_test_data',
    HDFS_USAGE_COL_NAME = 'hdfs_usage_data',
    HDFS_FILE_COUNTS_COL_NAME = 'hdfs_file_counts_data',
    HDFS_UPTIME_COL_NAME = 'hdfs_uptime_data',

    USER_PLATFORM_COL_NAME = 'userplatform_quota_usage',
    USER_YODA_COL_NAME = 'user_yoda_quota_usage',
    USER_YODA_WAREHOUSE_COL_NAME = 'user_yoda_warehouse_quota_usage',

    JIRA_USERNAME = 'JIRA_USERNAME',
    JIRA_PASSWORD = 'JIRA_PASSWORD',
    JIRA_OPTIONS = {'server': 'https://JIRA_API', 'verify': False},
    JIRA_ACCESS_LINK = 'https://JIRA_API:PORT_NUMBER/browse/',
    EMAIL = 'rakesh.kb@inmobi.com.com',
)
APPLICATION_PORT = 9010
APP_SETTINGS = dict()