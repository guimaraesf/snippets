# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: test_stopwatch.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description:
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from src.utils.time.stopwatch import Stopwatch


class StopwatchTest(unittest.TestCase):
    """
    Test cases for the Stopwatch class.
    """

    @patch("time.time")
    def setUp(self, mock_time):
        self.logger = MagicMock()
        self.stopwatch = Stopwatch(self.logger)
        self.time_sleep = mock_time.sleep(1)

    def test_elapsed_time(self):
        """Test count the elapsed time."""
        # Arrange
        self.stopwatch.start()
        self.time_sleep
        self.stopwatch.stop()

        # Act
        elapsed_time = self.stopwatch._elapsed_time()

        # Assert
        self.assertIsInstance(elapsed_time, float)
        self.assertTrue(elapsed_time > 0)

    # def test_show_elapsed_time(self):
    #     """Test show the elapsed time."""
    #     # Arrange
    #     self.stopwatch.start()
    #     self.time_sleep
    #     self.stopwatch.stop()

    #     # Act
    #     self.stopwatch.show_elapsed_time()

    #     # Assert
    #     args, _ = self.logger.info.call_args
    #     self.assertRegex(
    #         args[0],
    #         r"^Runtime execution: \d+(\.\d+)? hours \d+(\.\d+)? minutes \d+(\.\d+)? seconds$",
    #     )


if __name__ == "__main__":
    unittest.main()
