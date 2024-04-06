#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: app_log.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code centralizes all methods related to settings for using Logging
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
import logging
# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))


class AppLogging:
    """
    Logger class for the application.
    """

    def __init__(self, log_level=logging.INFO):
        # Configure log record format
        self.datefmt = "%Y/%m/%d %H:%M:%S.%f"[0:-3]
        self.message = "%(asctime)s %(levelname)s %(message)s"

        formatter_console = logging.Formatter(self.message, datefmt=self.datefmt)

        # Configure log handler to display in console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter_console)

        # Setting the global logging level to INFO
        logging.basicConfig(level=log_level, handlers=[])

        # Get root logger
        logger = logging.getLogger(__name__)

        # Remove all existing handlers from the root logger
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Add log handler to root logger
        logger.addHandler(console_handler)

        self.logger = logger

    def debug(self, message):
        """
        Log a debug message.
        """
        self.logger.debug(message)

    def info(self, message):
        """
        Log a info message.
        """
        self.logger.info(message)

    def warning(self, message):
        """
        Log a warning message.
        """
        self.logger.warning(message)

    def error(self, message):
        """
        Log a error message.
        """
        self.logger.error(message, exc_info=True)

    def critical(self, message):
        """
        Log a critical message.
        """
        self.logger.critical(message)
