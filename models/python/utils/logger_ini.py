# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# ================================================================================================
# Module: logger.py
# Author: Fernando Theodoro Guimarães
# Description:
# ================================================================================================
import logging
from logging import config


__PATH__ = "../../config/logging.ini"


class Logger:
    """
    Logger class for the application.
    """
    _instance = None

    @staticmethod
    def get_instance():
        """

        Returns
        -------

        """
        if Logger._instance is None:
            Logger()
        return Logger._instance

    def __init__(self, config_file: str = __PATH__) -> None:
        """
        Initializes the logger with the specified log level.

        Parameters
        ----------
        config_file : logging, optional
            The config file of logging to be used.
        """
        if Logger._instance is not None:
            raise Exception("This class is a Singleton")
        else:
            self.config_file = config_file
            self.logger = self._config_and_get_logger()
            Logger._instance = self

    def _config_and_get_logger(self) -> logging.Logger:
        """
        Returns the root logger.

        Returns
        -------
        logging.Logger
            The root logger.
        """
        logging.config.fileConfig(self.config_file)
        return logging.getLogger()
