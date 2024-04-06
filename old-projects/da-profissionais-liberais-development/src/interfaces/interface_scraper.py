# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: strategy.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description:
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from abc import ABC
from abc import abstractmethod
from typing import Optional

import aiohttp


class IScraper(ABC):
    """
    An abstract base class that defines the interface for a strategy.
    """

    @abstractmethod
    def create_blob_staging_path(self, prefix: str) -> str:
        """
        Creates a blob staging path.

        Args:
            prefix (str): The prefix for the blob staging path.

        Returns:
            str: The created blob staging path.
        """
        pass

    @abstractmethod
    def process_df_from_source(self, data: any, blob_name: str) -> None:
        """
        Processes a dataframe from a source.

        Args:
            data (any): The data to process.
            blob_name (str): The name of the blob.
        """
        pass

    @abstractmethod
    async def process_response(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        uf: Optional[str] = None,
        page: Optional[int] = None,
    ) -> int:
        """
        Processes a response.

        Args:
            session (aiohttp.ClientSession, optional): The client session. Defaults to None.
            uf (str, optional): The federal unit. Defaults to None.
            page (int, optional): The page number. Defaults to None.

        Returns:
            int: The result of the processing.
        """
        pass

    @abstractmethod
    async def scraper(self, connector: Optional[any] = None) -> None:
        """
        Scrapes data.

        Args:
            connector (optional): The connector to use for scraping. Defaults to None.
        """
        pass
