# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# ================================================================================================
# Module: logger.py
# Author: Fernando Theodoro Guimarães
# Description:
# ================================================================================================
import logging
from logging import config


__CONFIG_LOGGING__ = "../config/logging.ini"


class Logger:
    """
    Logger class for the application.
    """

    def __init__(self, config_file: str = __CONFIG_LOGGING__) -> None:
        """
        Initializes the logger with the specified log level.

        Parameters
        ----------
        config_file : logging, optional
            The config file of logging to be used.
        """
        self.config_file = config_file
        self.config_logger()

    def config_logger(self) -> logging.Logger:
        """
        Returns the root logger.

        Returns
        -------
        logging.Logger
            The root logger.
        """
        logging.config.fileConfig(self.config_file)
        logger = logging.getLogger()
        return logger
