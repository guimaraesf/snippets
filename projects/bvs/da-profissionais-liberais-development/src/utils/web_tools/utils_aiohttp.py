# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: utils_aiohttp.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for making asynchronous requests
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import importlib
import os
import sys
from asyncio.exceptions import CancelledError
from urllib.error import HTTPError
from typing import Optional
import aiohttp
from aiohttp.client_exceptions import ClientResponseError
from aiohttp.client_exceptions import ServerConnectionError

## When run in Dataproc workspace the original directory structure is not preserved.
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
URL_PARSER_MOD_PATH = get_module_path("src.utils.web_tools.url_parser")
Logger = importlib.import_module(LOGGER_MOD_PATH).Logger
ParserUrl = importlib.import_module(URL_PARSER_MOD_PATH).ParserUrl


class AsyncRequest(ParserUrl):
    """
    This class is responsible for making asynchronous requests.

    Args:
        ParserUrl (class): A class that parses URLs for requests.

    Raises:
        TypeError: If the request_session is not an instance of aiohttp.ClientSession.
        ValueError: If the URL is missing.
    """

    def __init__(self, request_session: aiohttp.ClientSession, url: str, logger_obj=Logger) -> None:
        """
        Initialize the AsyncRequest object.

        Args:
            request_session (aiohttp.ClientSession): The session for making requests.
            url (str): The URL to make requests to.
            logger (logging.Logger): The logger to be used for logging.
        Raises:
            TypeError: "Raises an error if `request_session` is not "aiohttp.ClientSession".
            ValueError: "Raises an error if missing `url`.
        """
        if not isinstance(request_session, aiohttp.ClientSession):
            raise TypeError("request_session must be an instance of aiohttp.ClientSession.")
        if not url:
            raise ValueError("Missing URL.")
        self.request_session = request_session
        self.url = url
        self.logger = logger_obj()
        super().__init__(url, logger_obj)

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
