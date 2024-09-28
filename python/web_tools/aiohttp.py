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


class AsyncRequest(ParserUrl):
    """
    This class is responsible for making asynchronous requests.

    Args:
        ParserUrl (class): A class that parses URLs for requests.

    Raises:
        TypeError: If the request_session is not an instance of aiohttp.ClientSession.
        ValueError: If the URL is missing.
    """

    def __init__(self, request_session: aiohttp.ClientSession, url: str) -> None:
        """
        Initialize the AsyncRequest object.

        Args:
            request_session (aiohttp.ClientSession): The session for making requests.
            url (str): The URL to make requests to.
        Raises:
            TypeError: "Raises an error if `request_session` is not "aiohttp.ClientSession".
            ValueError: "Raises an error if missing `url`.
        """
        if not isinstance(request_session, aiohttp.ClientSession):
            raise TypeError(
                "request_session must be an instance of aiohttp.ClientSession."
            )
        if not url:
            raise ValueError("Missing URL.")
        self.request_session = request_session
        self.url = url
        super().__init__(url)

    async def _get_response(self) -> tuple[aiohttp.ClientResponse, int, str]:
        """
        Get the response from the request.

        Returns:
            Tuple[aiohttp.ClientResponse, int, str]: The response from the request, along with the status and reason.

        Raises:
            ClientResponseError: If there is a client response error.
            ServerConnectionError: If there is a server connection error.
            CancelledError: If there is a cancelled error.
        """
        try:
            if self.url_is_allowed():
                response = await self.request_session.get(self.url, timeout=600)
                return response, response.status, response.reason
        except (
            ClientResponseError,
            ServerConnectionError,
        ) as client_error:
            raise (client_error)
        except (TimeoutError, CancelledError) as asyncio_error:
            raise (asyncio_error)

    async def fetch_json_response(self, encoding: Optional[str] = None) -> dict:
        """
        Fetch the JSON response from the request.

        Returns:
            dict: The JSON response from the request.

        Raises:
            HTTPError: If the status of the response is not 200.
        """
        response, status, reason = await self._get_response()
        async with response:
            if status != 200:
                return HTTPError(self.url, status, reason, None, None)
            return await response.json(encoding=encoding)
