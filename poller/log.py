from __future__ import unicode_literals, absolute_import
import logging
from poller import settings
logging.getLogger("requests").setLevel(logging.WARNING)

def setup_logger():
    # Set up Logging with rotation
    FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    app_logger = logging.getLogger('Poller Log')
    return app_logger
