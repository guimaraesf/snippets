# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: url_parser.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for validating the URL schema.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import importlib
import os
import sys
from urllib.parse import ParseResult
from urllib.parse import urlparse

# When run in Dataproc workspace the original directory structure is not preserved.
# These imports are useful for the code to be executed both locally and in another external environment.
PATH = os.path.dirname(os.path.abspath("__file__"))
sys.path.append(PATH)

def get_module_path(root_path: str):
    """
    This function returns the module path based on the given root path.

    Args:
    root_path (str): The root path of the module.

    Returns:
    str: If PATH does not start with sys.argv[9], it returns the root path. 
    """
    if not PATH.startswith(sys.argv[9]):
        return root_path
    else:
        return root_path.split(".")[-1]


LOGGER_MOD_PATH = get_module_path("src.utils.helpers.logger")
Logger = importlib.import_module(LOGGER_MOD_PATH).Logger


class ParserUrl:
    """
    Parser url for requests.
    """

    def __init__(self, url: str, logger_obj=Logger) -> None:
        """
        Initializes the instance of the class.

        Args:
            url (str): The URL that the instance will interact with.
            logger_obj (Logger, optional): An instance of the Logger class for logging. Defaults to Logger.
        """
        self.url = url
        self.logger = logger_obj()
        self.allowed_schemes = ["http", "https"]

    def _parse_url(self) -> ParseResult:
        """
        Parses the url.

        Returns:
            ParseResult: returning a 6-item named tuple. This corresponds to the general structure of a URL.
        """

        try:
            return urlparse(self.url)
        except AttributeError as e:
            self.logger.error(f"Error in URL: {e}")
            return None

    def url_is_allowed(self) -> bool:
        """
        Checks if the url format is allowed.

        Returns:
            bool: Return if url is allowed.
        """
        parsed_url = self._parse_url()
        if parsed_url.scheme in self.allowed_schemes:
            return True
        return False
