#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: file_downloader.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module contains classes to perform URL downloads.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import ssl
import urllib
import urllib.request
import urllib.error
from ssl import SSLError
from urllib.error import HTTPError, URLError
from http.client import IncompleteRead, InvalidURL


class SSLContextConfigurator:
    """
    Class for configuring the SSL context.
    """

    def __init__(self, check_hostname, verify_mode):
        self.check_hostname = check_hostname
        self.verify_mode = verify_mode

    def configure_ssl_context(self):
        """
        Configures the SSL context.
        """
        try:
            context = ssl.create_default_context()
            context.check_hostname = self.check_hostname
            context.verify_mode = self.verify_mode
            return context
        except SSLError as error:
            print(f"SSLError: {error.reason}")


class ParserUrl:
    """
    Parser for requests
    """

    def __init__(self, url, logger):
        self.url = url
        self.logger = logger
        self.allowed_schemes = ["http", "https"]

    def parse_url(self):
        """
        Parses the url.
        """
        try:
            return urllib.parse.urlparse(self.url)
        except (InvalidURL, URLError) as url_error:
            self.logger.error(f"Error in URL: {url_error}")

    def url_is_allowed(self):
        """
        Checks if the url is allowed.
        """
        parsed_url = self.parse_url
        if parsed_url.scheme in self.allowed_schemes:
            return True
        return False


class UrllibDownloader(ParserUrl):
    """
    Class for downloading files.
    """

    def __init__(self, url, context, path_where_to_save, logger, request=urllib.request.Request, urlopen=urllib.request.urlopen):
        self.url = url
        self.context = context
        self.path_where_to_save = path_where_to_save
        self.logger = logger
        self.request = request
        self.urlopen = urlopen
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0;Win64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
        }
        super().__init__(url, logger)

    def __create_request_obj(self):
        """
        Create request object.
        """
        return self.request(url=self.url, headers=self.headers)

    def __handle_http_error(self, error, request=None):
        if error.status != 403:
            self.logger.error(f"HTTP Error {error.status}: {error.reason}")
        self.logger.warning("Creating a request object for a new request.")
        if request is None:
            request = self.__create_request_obj()
        try:
            return self.urlopen(url=request, context=self.context)
        except HTTPError as error:
            self.logger.error(f"HTTP Error {error.status}: {error.reason}")
        return None

    def __get_response(self):
        """
        Executes the request.
        """
        try:
            self.logger.info("Verifying if URL is valid.")
            if not self.url_is_allowed:
                self.logger.error("URL scheme is not allowed.")
                return None

            self.logger.info("URL scheme is allowed (HTTP or HTTPS).")
            try:
                return self.urlopen(url=self.url, context=self.context)
            except HTTPError as error:
                return self.__handle_http_error(error)
        except HTTPError as error:
            self.logger.error(f"HTTP Error {error.status}: {error.reason}")

    @staticmethod
    def __get_status_code(response):
        """
        Get the status code.
        """
        return response.status

    @staticmethod
    def __get_reason(response):
        """
        Get the reason.
        """
        return response.reason

    def __process_response(self, response):
        """
        Processes the response.
        """
        with open(self.path_where_to_save, "wb") as outfile:
            outfile.write(response)
            self.logger.info("File was downloaded successfully.")

    def _verify_successful_request(self, response, status_code, reason):
        """
        Verifies the request.
        """
        self.logger.info("Verifying that the request was successful.")
        if status_code == 200:
            self.logger.info(
                f"The request was successful: HTTP {status_code}: {reason}"
            )
            self.logger.info(f"Downloading file to: {self.path_where_to_save}")
            self.__process_response(response.read())
        else:
            self.logger.error(
                f"HTTP Error {status_code}: {reason}"
            )

    def run_download(self):
        """
        Downloads the file.
        """
        try:
            response = self.__get_response()
            if response is not None:
                status_code = self.__get_status_code(response)
                reason = self.__get_reason(response)
                self._verify_successful_request(response, status_code, reason)
        except IncompleteRead as error:
            self.logger.error(f"IncompleteRead: Expected: {error.expected}")
