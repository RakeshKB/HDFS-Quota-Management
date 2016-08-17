#!/usr/bin/python
import sys
import json
import logging
import datetime
import tornado.web
import tornado.ioloop
import tornado.options
import tornado.httpserver
from api_server import settings
import api_server.api_handler

from tornado.log import enable_pretty_logging


def run():
    # Bootstrap singletons
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, settings.LOG_LEVEL))
    enable_pretty_logging(logger=logger)

    application = tornado.web.Application(
        [
            (r"/api/details/Test", api_server.api_handler.TestDataHandler),
            (r"/api/details/test", api_server.api_handler.Test),
            (r"/api/jira/create_issue", api_server.api_handler.CreateJiraHandler),
            (r"/api/jira/edit_issue", api_server.api_handler.EditJiraHandler),
            (r"/api/view/hdfs_usage", api_server.api_handler.HDFSUsageHandler),
            (r"/api/view/file_counts", api_server.api_handler.FileCountsHandler),
            (r"/api/quota/user_platform", api_server.api_handler.UserPlatformHandler),
            (r"/api/quota/user_yoda", api_server.api_handler.UserYodaHandler),
            (r"/api/quota/user_yoda_warehouse", api_server.api_handler.UserYodaWarehouseHandler),
            (r"/api/view/hdfs_uptime", api_server.api_handler.HDFSUptimeHandler)
        ], **settings.APP_SETTINGS)
    logger.info('Starting View Master at port:%s' % settings.APPLICATION_PORT)
    logger.info('MongoDB Host:%s, Port:%s' % (settings.DB['MONGO_HOST'], settings.DB['MONGO_PORT']))

    application.listen(settings.APPLICATION_PORT)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    run()
