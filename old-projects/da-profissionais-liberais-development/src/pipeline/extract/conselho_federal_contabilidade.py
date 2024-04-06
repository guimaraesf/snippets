# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: conselho_federal_contabilidade.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for scraping data from the Conselho Federal de Contabilidade
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import asyncio
import importlib
import os
import ssl
import sys

import aiohttp
import nest_asyncio
from aiohttp import TCPConnector
from google.cloud import storage

# When run in Dataproc workspace the original directory structure is not preserved.
# These imports are useful for the code to be executed both locally and in another external environment.
PATH = os.path.dirname(os.path.abspath("__file__"))
sys.path.append(os.path.dirname(os.path.abspath("__file__")))


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


ASYNC_MOD_PATH = get_module_path("src.utils.web_tools.utils_aiohttp")
GCS_CLIENT_MOD_PATH = get_module_path("src.utils.gcp.storage.gcs_client")
GCS_MANAGER_MOD_PATH = get_module_path("src.utils.gcp.storage.gcs_manager")
LOGGER_MOD_PATH = get_module_path("src.utils.helpers.logger")
PANDAS_FILE_HANDLE = get_module_path("src.utils.dataframe.pandas.file_handler")
SSL_CONFIG_MOD_PATH = get_module_path("src.utils.web_tools.ssl_context")
ISCRAPER_MOD_PATH = get_module_path("src.interfaces.interface_scraper")
STOPWATCH_MOD_PATH = get_module_path("src.utils.time.stopwatch")
UTILS_MOD_PATH = get_module_path("src.utils.helpers.utils")
VARIABLES_MOD_PATH = get_module_path("src.utils.helpers.variables")

AsyncRequest = importlib.import_module(ASYNC_MOD_PATH).AsyncRequest
GcsClient = importlib.import_module(GCS_CLIENT_MOD_PATH).GcsClient
GcsManager = importlib.import_module(GCS_MANAGER_MOD_PATH).GcsManager
Logger = importlib.import_module(LOGGER_MOD_PATH).Logger
PandasFileHandle = importlib.import_module(PANDAS_FILE_HANDLE).PandasFileHandle
SslContextConfigurator = importlib.import_module(SSL_CONFIG_MOD_PATH).SslContextConfigurator
IScraper = importlib.import_module(ISCRAPER_MOD_PATH).IScraper
Stopwatch = importlib.import_module(STOPWATCH_MOD_PATH).Stopwatch
UtilsDataproc = importlib.import_module(UTILS_MOD_PATH).UtilsDataproc
Variables = importlib.import_module(VARIABLES_MOD_PATH).Variables


class ScraperCfc(IScraper):
    """
    Class responsible for scraping concrete CFC data.
    """

    def __init__(
        self,
        type_list: str,
        url: str,
        bucket_obj: storage.Bucket,
        logger_obj=Logger,
        gcs_manager=GcsManager,
        pandas_file_handle=PandasFileHandle,
        async_request=AsyncRequest,
        variables=Variables,
        ssl_configurator=SslContextConfigurator,
    ) -> None:
        """
        Initializes the ScraperCfc class.

        Args:
            type_list (str): The type of list for the scraper.
            url (str): The URL to scrape data from.
            bucket (storage.Bucket): The Google Cloud Storage bucket to use.
            logger (Logger, optional): The logger to use. Defaults to Logger.
            gcs_manager (GcsManager, optional): The Google Cloud Storage manager to use. Defaults to GcsManager.
            pandas_file_handle (PandasFileHandle, optional): The file handler to use. Defaults to PandasFileHandle.
            async_request (AsyncRequest, optional): The asynchronous request to use. Defaults to AsyncRequest.
            variables (Variables, optional): The variables to use. Defaults to Variables.
            ssl_configurator (SslContextConfigurator, optional): The ssl configuration to use. Defaults to SslContextConfigurator.
        Raises:
            ValueError: Raises an error if `type_list` is not "Profissional" or "Empresa".
        """
        if type_list not in ["Profissional", "Empresa"]:
            raise ValueError("type_list must be Profissional or Empresa")
        self.type_list = type_list
        self.url = url
        self.bucket = bucket_obj
        self.logger = logger_obj()
        self.gcs_manager = gcs_manager(self.bucket)
        self.async_request = async_request
        self.pandas_file_handle = pandas_file_handle()
        self.variables = variables
        self.ssl_configurator = ssl_configurator()
        self.skip = 0
        self.take = 0

    def create_blob_staging_path(self, uf: str) -> str:
        """
        Create a blob path for a specific file in Google Cloud Storage.

        Args:
            uf (str): The unique identifier for the file.

        Returns:
            str: The blob path in Google Cloud Storage.
        """
        layer_name = self.variables.STAGING_NAME
        dir_name = self.variables.DIR_NAME_CFC
        file_name = self.variables.FILENAME.format(f"{self.type_list.lower()}/{uf.lower()}", "cfc")
        blob_name = self.variables.BLOB_NAME.format(layer_name, dir_name, file_name)
        return blob_name

    def process_df_from_source(self, data: dict, uf: str, blob_name: str) -> None:
        """
        Processes a DataFrame from a JSON object and saves it to a CSV file.

        Args:
            data (dict): The data to be processed.
            uf (str): The state to process the response for.
            blob_name (str): The path where the file will be saved in bucket.
        """
        self.logger.info("Reading as DataFrame from JSON response.")
        df = self.pandas_file_handle.normalize_json_file(data)

        self.logger.info(f"Total number of records for {uf}: {df.shape[0]}")
        csv_data = self.pandas_file_handle.convert_to_csv_file(df=df)

        self.logger.info("Uploading CSV files in the bucket.")
        blob = self.gcs_manager.create_blob(blob_name)
        self.gcs_manager.blob_upload_from_string(blob, csv_data)
        self.logger.info(f"File were uploaded successfully to GCS: {blob.name}")

    async def process_response(self, session: aiohttp.ClientSession, uf: str) -> None:
        """
        Processes the response for a given session and state.

        Args:
            session (aiohttp.ClientSession): The session to process the response for.
            uf (str): The state to process the response for.
        """
        url = self.url.format(self.type_list, uf, self.skip, self.take)
        request = self.async_request(session, url)
        json_response = await request.fetch_json_response()
        data = json_response.get("data")

        if not data:
            self.logger.warning(f"The request from {uf} returned empty.")
            return None

        blob_path = self.create_blob_staging_path(uf)
        self.process_df_from_source(data, uf, blob_path)
        await asyncio.sleep(5)

    def set_ssl_context(self) -> ssl.SSLContext:
        """
        A method for creating and setting an SSL context for the client.

        Returns:
            ssl.SSLContext: An SSL context object with the specified parameters.
        """
        return self.ssl_configurator.create_context(True, ssl.CERT_REQUIRED)

    async def scraper(self) -> None:
        """
        Main method that runs all tasks for all states.
        """
        connector = TCPConnector(ssl=self.set_ssl_context())
        async with aiohttp.ClientSession(
            connector=connector, headers=self.variables.HEADERS
        ) as session:
            tasks = [self.process_response(session, uf) for uf in self.variables.UF]
            await asyncio.gather(*tasks)


if __name__ == "__main__":
    logger = Logger()

    stopwatch = Stopwatch()
    stopwatch.start()

    nest_asyncio.apply()

    utils_dataproc = UtilsDataproc()
    BUCKET_ID = utils_dataproc.get_bucket_id
    PROJECT_ID = utils_dataproc.get_project_id

    gcs_client = GcsClient(PROJECT_ID, BUCKET_ID)
    client = gcs_client.instantiate_client()
    bucket = gcs_client.create_bucket_obj(client)

    logger.info("Starting extraction process for Professional.")
    scraper_pf = ScraperCfc("Profissional", Variables.URL_CFC, bucket)
    asyncio.run(scraper_pf.scraper())

    logger.info("Starting extraction process for Enterprise.")
    scraper_pj = ScraperCfc("Empresa", Variables.URL_CFC, bucket)
    asyncio.run(scraper_pj.scraper())

    stopwatch.stop()
    stopwatch.show_elapsed_time()
