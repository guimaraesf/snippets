# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: utils_aiohttp.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This module is responsible for making asynchronous requests
# ================================================================================================
from asyncio.exceptions import CancelledError
from urllib.error import HTTPError
from typing import Optional
import aiohttp
from aiohttp.client_exceptions import ClientResponseError
from aiohttp.client_exceptions import ServerConnectionError
from urllib.parse import urlparse


class AioHttp:
    """
    This class is responsible for making asynchronous requests.

    Raises:
        TypeError: If the session is not an instance of aiohttp.ClientSession.
        ValueError: If the URL is missing.
    """

    def __init__(self, session: aiohttp.ClientSession, url: str) -> None:
        """
        Initialize the AsyncRequest object.

        Args:
            session (aiohttp.ClientSession): The session for making requests.
            url (str): The URL to make requests to.
        """
        self.session = session
        self.url = url

    def url_is_allowed(self) -> bool:
        """
        Checks if the url format is allowed.

        Returns:
            bool: Return if url is allowed.
        """
        parsed_url = urlparse(self.url)
        if parsed_url.scheme in ["https"]:
            return True
        return False

    async def fetch_json_response(self, encoding: Optional[str] = None) -> dict:
        """
        Fetch the JSON response from the request.

        Returns:
            dict: The JSON response from the request.

        Raises:
            HTTPError: If the status of the response is not 200.
        """
        if self.url_is_allowed():
            response = await self.session.get(self.url, timeout=600)
            status, reason = response.status, response.reason
            async with response:
                if status != 200:
                    return HTTPError(self.url, status, reason, None, None)
                return await response.json(encoding=encoding)
