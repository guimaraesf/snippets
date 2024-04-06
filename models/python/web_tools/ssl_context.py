# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: ssl_context.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This module is responsible for configuring the SSL context
# ================================================================================================
import ssl
from ssl import SSLContext
from ssl import VerifyMode


class SslContextConf:
    """
    Class for configuring the SSL context.
    """

    def __init__(self) -> None:
        """
        Initializes the SslContextConfigurator class.
        """
        pass

    @staticmethod
    def create_context(check_hostname: bool, verify_mode: VerifyMode) -> SSLContext:
        """
        Creates an SSL context with the given parameters.

        Args:
            check_hostname (bool): If true, the hostname will be verified.
            verify_mode (VerifyMode): The SSL verification mode to use.

        Returns:
            SSLContext: The created SSL context.
        """
        context = ssl.create_default_context()
        context.check_hostname = check_hostname
        context.verify_mode = verify_mode
        return context
