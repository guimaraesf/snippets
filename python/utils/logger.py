# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# ================================================================================================
# Module: logger.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@ime.usp.br
# Description:
# ================================================================================================
import os
import logging
from logging import config

__PATH__ = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../config/logging.ini")
)


class Logger:
    """
    Logger class for the application.
    """

    _instance = None

    @staticmethod
    def init(config_file: str = __PATH__):
        """
        Returns the singleton instance of the logger, creating it if necessary.

        Parameters:
        ----------
        config_file : str, optional
            The configuration file for the logger.

        Returns:
        -------
        Logger
            The singleton instance of the logger.
        """

        if Logger._instance is None:
            Logger._instance = Logger(config_file)
        return Logger._instance.logger

    def __init__(self, config_file: str):
        """
        Initializes the logger with the specified configuration file.

        Parameters:
        ----------
        config_file : str
            The configuration file for the logger.
        """

        self.logger = logging.getLogger()
        logging.config.fileConfig(config_file)
