"""Utility for setting up logging"""
import logging


class LogUtil(object):

    _instance = None

    def __new__(cls, logger_name, log_level=logging.INFO):
        if not cls._instance:
            cls._instance = super(LogUtil, cls).__new__(cls)
            log_format = '[%(asctime)s] - %(thread)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s'
            logger = logging.getLogger(logger_name)
            console_handler = logging.StreamHandler()
            console_format = logging.Formatter(log_format)
            console_handler.setFormatter(console_format)
            logger.addHandler(console_handler)
            logger.setLevel(log_level)
        return cls._instance
