# This Python file uses the following encoding: utf-8

#!/usr/bin/python
# -*- coding: utf-8 -*-

LOG_LEVEL = 'DEBUG'

INFO = dict(
    #Details to fetch data from Grafana

    MONGO_DB_NAME = 'hdfs_quota_management',
    HDFS_USAGE_METRICS_BASE_QUERY = 'http://metrics-web.grid.colo.company.com/render/?target=',

    HDFS_USAGE_COL_NAME = 'hdfs_usage_data',
    HDFS_FILE_COUNTS_COL_NAME = 'hdfs_file_counts_data',

    USER_PLATFORM_COL_NAME = 'userplatform_quota_usage',
    USER_YODA_COL_NAME = 'user_yoda_quota_usage',
    USER_YODA_WAREHOUSE_COL_NAME = 'user_yoda_warehouse_quota_usage',
    HDFS_UPTIME_COL_NAME = 'hdfs_uptime_data'

    )