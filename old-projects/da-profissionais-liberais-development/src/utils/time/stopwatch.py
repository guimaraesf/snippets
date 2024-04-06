# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: stopwatch.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for calculating the execution time of tasks.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import importlib
import os
import sys
import time

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


class Stopwatch:
    """
    A Stopwatch class that can be used to measure the time of operations.
    """

    def __init__(self, logger_obj=Logger) -> None:
        """Initializes the Stopwatch object.

        Args:
            logger (logging.Logger): The logger to be used for logging.
        """
        self.logger = logger_obj()
        self.start_time = None
        self.end_time = None

    def start(self):
        """
        Start the Stopwatch.
        """
        self.start_time = time.perf_counter()

    def stop(self):
        """
        Stop the Stopwatch.
        """
        self.end_time = time.perf_counter()

    def _elapsed_time(self):
        """
        Calculate the elapsed time.
        """
        return self.end_time - self.start_time

    def show_elapsed_time(self):
        """
        Show the elapsed time.
        """
        elapsed_time = self._elapsed_time()

        hours = round(elapsed_time // 3600, 2)
        minutes = round((elapsed_time % 3600) // 60, 2)
        seconds = round(elapsed_time % 60, 4)

        self.logger.info(f"Runtime execution: {hours} hours {minutes} minutes {seconds} seconds")
