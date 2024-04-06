# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: logger.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code centralizes all methods related to settings for using Logging
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import logging
import sys


class Logger:
    """
    Logger class for the application.
    """

    def __init__(self, log_level: logging = logging.INFO) -> None:
        """
        Initializes the logger with the specified log level.
        This method configures the log record format and the log handler to display in console.
        It also sets the global logging level and gets the root logger.

        Args:
            log_level (logging, optional): The level of logging to be used. Defaults to logging.INFO.
        """
        self.log_level = log_level
        self.datefmt = "%Y/%m/%d %H:%M:%S.%f"[0:-3]
        self.message = "%(asctime)s %(levelname)s %(message)s"
        self.configure_logger()

    def configure_logger(self) -> None:
        """
        Configures the logger.
        """
        formatter_console = self._get_formatter()
        console_handler = self._get_console_handler()
        self._set_level(console_handler)
        self._set_formatter(console_handler, formatter_console)
        self._set_global_log_level()
        logger = self._get_root_logger()
        self._remove_all_existing_handlers(logger)
        self._add_log_handler_to_root_logger(logger, console_handler)
        self.logger = logger

    def _get_formatter(self) -> logging.Formatter:
        """
        Returns a formatter for the logger.

        Returns:
            logging.Formatter: A formatter with a specific date format and message format.
        """
        return logging.Formatter(self.message, datefmt=self.datefmt)

    def _get_console_handler(self) -> logging.StreamHandler:
        """
        Returns a console handler for the logger.

        Returns:
            logging.StreamHandler: A console handler with a specific log level and formatter.
        """
        return logging.StreamHandler(sys.stdout)

    def _set_level(self, console_handler: logging.StreamHandler) -> None:
        """
        Set the logging level of this handler.

        Args:
            console_handler (logging.StreamHandler): A console handler with a specific log level and formatter.
        Raises:
            TypeError: A parameter level must be a int or str. (e.g. INFO or 20)
        """
        if any(isinstance(self.log_level, type) for type in (int, str)):
            console_handler.setLevel(self.log_level)
        else:
            raise TypeError("level must be a int or str.")

    def _set_formatter(
        self,
        console_handler: logging.StreamHandler,
        formatter_console: logging.Formatter,
    ) -> None:
        """
        Set the formatter for this handler.

        Args:
            console_handler (logging.StreamHandler): A console handler with a specific log level and formatter.
            formatter_console (logging.Formatter): A formatter with a specific date format and message format.
        """
        console_handler.setFormatter(formatter_console)

    def _set_global_log_level(self) -> None:
        """
        Sets the global logging level to the log level specified during initialization.
        """
        logging.basicConfig(level=self.log_level, handlers=[])

    def _get_root_logger(self) -> logging.Logger:
        """
        Returns the root logger.

        Returns:
            logging.Logger: The root logger.
        """
        return logging.getLogger(__name__)

    def _remove_all_existing_handlers(self, logger: logging.Logger) -> None:
        """
        Removes all existing handlers from the logger.

        Args:
            logger (logging.Logger): The logger from which to remove all handlers.
        """
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

    def _add_log_handler_to_root_logger(
        self, logger: logging.Logger, console_handler: logging.StreamHandler
    ) -> None:
        """
        Adds a log handler to the root logger.

        Args:
            logger (logging.Logger): The logger to which to add the handler.
            console_handler (logging.StreamHandler): The handler to add to the logger.
        """
        logger.addHandler(console_handler)

    def debug(self, message) -> None:
        """Log a debug message.

        Args:
            message (str): Message to register.
        """
        self.logger.debug(message)

    def info(self, message) -> None:
        """Log a info message.

        Args:
            message (str): Message to register.
        """
        self.logger.info(message)

    def warning(self, message) -> None:
        """Log a warning message.

        Args:
            message (str): Message to register.
        """
        self.logger.warning(message)

    def error(self, message) -> None:
        """Log a error message.

        Args:
            message (str): Message to register.
        """
        self.logger.error(message)

    def critical(self, message) -> None:
        """Log a critical message.

        Args:
            message (str): Message to register.
        """
        self.logger.critical(message)
