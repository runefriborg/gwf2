import logging
import os


if os.getenv('GWF_DEBUG', False):
    logging.basicConfig(level=logging.DEBUG)
