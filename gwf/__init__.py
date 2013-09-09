import logging
import os


if not os.getenv('GWF_DEBUG', False):
    logging.basicConfig(level=logging.DEBUG)
