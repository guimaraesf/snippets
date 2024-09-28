# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: stopwatch.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This module is responsible for calculating the execution time of tasks.
# ================================================================================================
import time


class Stopwatch:
    """
    A Stopwatch class that can be used to measure the time of operations.
    """

    def __init__(self) -> None:
        """
        Initializes the Stopwatch object.
        """
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

    def show_elapsed_time(self) -> tuple[float, float, float]:
        """
        Show the elapsed time.

        Returns:
            Return a tuple with times of tasks (hours, minutes and seconds).
        """
        elapsed_time = self._elapsed_time()

        hours = round(elapsed_time // 3600, 2)
        minutes = round((elapsed_time % 3600) // 60, 2)
        seconds = round(elapsed_time % 60, 4)
        return hours, minutes, seconds
