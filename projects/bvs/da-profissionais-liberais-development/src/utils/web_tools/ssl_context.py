# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: ssl_context.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for configuring the SSL context
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import importlib
import os
import ssl
import sys
from ssl import SSLContext
from ssl import SSLError
from ssl import VerifyMode

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


class SslContextConfigurator:
    """
    Class for configuring the SSL context.
    """

    def __init__(self, logger_obj=Logger):
        """
        Initializes the SslContextConfigurator class.

        Args:
            logger (logging.Logger): The logger to use for logging.
        """
        self.logger = logger_obj()

    def create_context(self, check_hostname: bool, verify_mode: VerifyMode) -> SSLContext:
        """
        Creates an SSL context with the given parameters.

        Args:
            check_hostname (bool): If true, the hostname will be verified.
            verify_mode (VerifyMode): The SSL verification mode to use.

        Returns:
            SSLContext: The created SSL context.
        """
        try:
            context = ssl.create_default_context()
            context.check_hostname = check_hostname
            context.verify_mode = verify_mode
            return context
        except SSLError as e:
            self.logger.info(f"SSLError: {e.reason}")
            return None
