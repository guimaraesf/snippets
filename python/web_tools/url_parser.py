# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: url_parser.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This module is responsible for validating the URL schema.
# ================================================================================================
from urllib.parse import urlparse


class ParserUrl:
    """
    Parser url for requests.
    """

    def __init__(self, url: str) -> None:
        """
        Initializes the instance of the class.

        Args:
            url (str): The URL that the instance will interact with.
        """
        self.url = url
        self.allowed_schemes = ["http", "https"]

    def url_is_allowed(self) -> bool:
        """
        Checks if the url format is allowed.

        Returns:
            bool: Return if url is allowed.
        """
        parsed_url = urlparse(self.url)
        if parsed_url.scheme in self.allowed_schemes:
            return True
        return False
