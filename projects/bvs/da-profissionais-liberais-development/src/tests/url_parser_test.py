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
import os
import sys
import unittest
from unittest.mock import Mock
from urllib.parse import ParseResult
from urllib.parse import ParseResultBytes

from src.utils.web_tools.url_parser import ParserUrl


class TestParserUrl(unittest.TestCase):
    def setUp(self):
        self.logger = Mock()
        self.parser = ParserUrl("https://example.com", self.logger)

    def test_parse_url(self):
        # Act
        result = self.parser._parse_url()

        # Assert
        self.assertIsInstance(result, ParseResult)

    def test_parse_url_is_none(self):
        # Arrange
        self.parser.url = None

        # Act
        result = self.parser._parse_url()
        self.assertIsInstance(result, ParseResultBytes)

    def test_parse_url_is_int(self):
        # Arrange
        self.parser.url = 1

        # Act
        result = self.parser._parse_url()

        # Assert
        self.assertRaises(AttributeError)
        self.logger.error.assert_called_once()
        args, _ = self.logger.error.call_args
        self.assertRegex(args[0], r"Error in URL: (.*)")

    def test_url_is_allowed(self):
        # Act
        result = self.parser.url_is_allowed()

        # Assert
        self.assertTrue(result)

    def test_url_is_not_allowed(self):
        # Arrange
        self.parser.url = "ftp://example.com"

        # Act
        result = self.parser.url_is_allowed()

        # Assert
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
