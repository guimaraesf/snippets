# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: test_logger.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description:
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import unittest
from logging import Formatter
from logging import StreamHandler
from unittest.mock import MagicMock
from unittest.mock import patch

from src.utils.helpers.logger import Logger


class TestLogger(unittest.TestCase):
    def setUp(self):
        self.logger = Logger()
        self.log_level_default = 20
        self.handler_pattern = StreamHandler()
        self.formatter_pattern = Formatter(
            fmt="%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y/%m/%d %H:%M:%S.%f"[0:-3],
        )

    @patch("logging.Formatter")
    def test_get_formatter(self, mock_formatter):
        # Act
        self.logger._get_formatter()

        # Assert
        mock_formatter.assert_called_once_with(self.logger.message, datefmt=self.logger.datefmt)


if __name__ == "__main__":
    unittest.main()
