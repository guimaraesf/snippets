# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: conselho_federal_educacao_fisica.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for scraping data from the Conselho Federal Educação Física
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
# import asyncio
# import importlib
# import os
# import ssl
# import sys
# import aiohttp
# import nest_asyncio
# from aiohttp import TCPConnector
# from google.cloud import storage
# # When run in Dataproc workspace the original directory structure is not preserved.
# # These imports are useful for the code to be executed both locally and in another external environment.
# PATH = os.path.dirname(os.path.abspath("__file__"))
# sys.path.append(PATH)
# def get_module_path(root_path: str):
#     """
#     This function returns the module path based on the given root path.
#     Args:
#     root_path (str): The root path of the module.
#     Returns:
#     str: If PATH does not start with sys.argv[9], it returns the root path.
#     """
#     if not PATH.startswith(sys.argv[9]):
#         return root_path
#     else:
#         return root_path.split(".")[-1]
# ASYNC_MOD_PATH = get_module_path("src.utils.web_tools.utils_aiohttp")
# COLUMNS_MOD_PATH = get_module_path("src.utils.dataframe.pyspark.schema.columns")
# GCS_CLIENT_MOD_PATH = get_module_path("src.utils.gcp.storage.gcs_client")
# GCS_MANAGER_MOD_PATH = get_module_path("src.utils.gcp.storage.gcs_manager")
# LOGGER_MOD_PATH = get_module_path("src.utils.helpers.logger")
# PANDAS_FILE_HANDLE = get_module_path("src.utils.dataframe.pandas.file_handler")
# SSL_CONFIG_MOD_PATH = get_module_path("src.utils.web_tools.ssl_context")
# ISCRAPER_MOD_PATH = get_module_path("src.interfaces.interface_scraper")
# STOPWATCH_MOD_PATH = get_module_path("src.utils.time.stopwatch")
# UTILS_MOD_PATH = get_module_path("src.utils.helpers.utils")
# VARIABLES_MOD_PATH = get_module_path("src.utils.helpers.variables")
# AsyncRequest = importlib.import_module(ASYNC_MOD_PATH).AsyncRequest
# Columns = importlib.import_module(COLUMNS_MOD_PATH).Columns
# GcsClient = importlib.import_module(GCS_CLIENT_MOD_PATH).GcsClient
# GcsManager = importlib.import_module(GCS_MANAGER_MOD_PATH).GcsManager
# Logger = importlib.import_module(LOGGER_MOD_PATH).Logger
# PandasFileHandle = importlib.import_module(PANDAS_FILE_HANDLE).PandasFileHandle
# SslContextConfigurator = importlib.import_module(SSL_CONFIG_MOD_PATH).SslContextConfigurator
# IScraper = importlib.import_module(ISCRAPER_MOD_PATH).IScraper
# Stopwatch = importlib.import_module(STOPWATCH_MOD_PATH).Stopwatch
# UtilsDataproc = importlib.import_module(UTILS_MOD_PATH).UtilsDataproc
# Variables = importlib.import_module(VARIABLES_MOD_PATH).Variables
# class ScraperConfef(IScraper):
#     """
#     Class responsible for scraping concrete Susep data.
#     """
#     def __init__(
#         self,
#         url: str,
#         bucket_obj: storage.Bucket,
#         logger_obj=Logger,
#         gcs_manager=GcsManager,
#         pandas_file_handle=PandasFileHandle,
#         async_request=AsyncRequest,
#         columns=Columns,
#         variables=Variables,
#     ) -> None:
#         """
#         Initializes the ScraperConfef class.
#         Args:
#             url (str): The URL to scrape data from.
#             bucket (storage.Bucket): The Google Cloud Storage bucket to use.
#             logger (Logger, optional): The logger to use. Defaults to Logger.
#             gcs_manager (GcsManager, optional): The Google Cloud Storage manager to use. Defaults to GcsManager.
#             pandas_file_handle (PandasFileHandle, optional): The file handler to use. Defaults to PandasFileHandle.
#             async_request (AsyncRequest, optional): The asynchronous request to use. Defaults to AsyncRequest.
#             variables (Variables, optional): The variables to use. Defaults to Variables.
#         """
#         self.url = url
#         self.logger = logger_obj()
#         self.bucket = bucket_obj
#         self.gcs_manager = gcs_manager(self.bucket)
#         self.async_request = async_request
#         self.pandas_file_handle = pandas_file_handle()
#         self.columns = columns
#         self.variables = variables
#         self.total_sum = 0
#         self.start = 0
#         self.length = 15  # Number of records returned
#         self.page = 1
#     def create_blob_staging_path(self, uf: str) -> str:
#         """
#         Create a blob path for a specific file in Google Cloud Storage.
#         Args:
#             uf (str): The state to process the response for.
#         Returns:
#             str: The blob path in Google Cloud Storage.
#         """
#         layer_name = self.variables.STAGING_NAME
#         dir_name = self.variables.DIR_NAME_CONFEF
#         file_name = self.variables.FILENAME.format(uf.lower(), f"confef_{str(self.page).zfill(4)}")
#         blob_name = self.variables.BLOB_NAME.format(layer_name, dir_name, file_name)
#         return blob_name
#     def process_df_from_source(self, data: list, blob_name: str) -> None:
#         """
#         Processes a DataFrame from a JSON object and saves it to a CSV file.
#         Args:
#             data (list): The data to be processed.
#             blob_name (str): The path where the file will be saved in bucket.
#         """
#         self.logger.info("Reading as DataFrame from JSON response.")
#         columns_name = list(self.columns.TABLE_INFOS_CONFEF.keys())
#         df = self.pandas_file_handle.create_df(data, columns_name)
#         csv_data = self.pandas_file_handle.convert_to_csv_file(df=df)
#         self.logger.info("Uploading CSV files in the bucket.")
#         blob = self.gcs_manager.create_blob(blob_name)
#         self.gcs_manager.blob_upload_from_string(blob, csv_data)
#         self.logger.info(f"File were uploaded successfully to GCS: {blob.name}")
#     async def process_response(self, session: aiohttp.ClientSession, uf: str) -> None:
#         """
#         Processes the response for a given session and state.
#         Args:
#             session (aiohttp.ClientSession): The session to process the response for.
#             uf (str): The state to process the response for.
#         """
#         url = self.url.format(uf, self.start, self.length)
#         request = self.async_request(session, url)
#         json_response = await request.fetch_json_response(encoding="utf-8-sig")
#         data = json_response.get("data")
#         total_records = json_response.get("recordsFiltered")
#         if not data:
#             self.logger.warning("The request returned empty.")
#             return None
#         blob_name = self.create_blob_staging_path(uf)
#         self.process_df_from_source(data, blob_name)
#         self.total_sum += len(data)
#         self.page += 1
#         if uf in {"SP", "MG", "RJ", "RS", "BA"}:
#             await asyncio.sleep(60)
#         await asyncio.sleep(40)
#         return total_records
#     async def scraper(self, connector: TCPConnector) -> None:
#         """
#         Main method that runs all tasks.
#         """
#         async with aiohttp.ClientSession(
#             connector=connector, headers=self.variables.HEADERS, raise_for_status=True
#         ) as session:
#             for uf in self.variables.UF:
#                 while True:
#                     total_records = await self.process_response(session, uf)
#                     if total_records is not None:
#                         self.start += self.length
#                         if self.start + self.length > total_records:
#                             self.length = total_records - self.start
#                         if self.total_sum >= total_records:
#                             self.logger.info("Finished scraping.")
#                             self.start, self.total_sum, self.page = (0, 0, 1)
#                             break
#                     else:
#                         self.logger.warning("Skipping for next.")
#                         break
# if __name__ == "__main__":
#     logger = Logger()
#     stopwatch = Stopwatch()
#     stopwatch.start()
#     nest_asyncio.apply()
#     utils_dataproc = UtilsDataproc()
#     BUCKET_ID = utils_dataproc.get_bucket_id
#     PROJECT_ID = utils_dataproc.get_project_id
#     gcs_client = GcsClient(PROJECT_ID, BUCKET_ID)
#     client = gcs_client.instantiate_client()
#     bucket = gcs_client.create_bucket_obj(client)
#     ssl_context_configurator = SslContextConfigurator()
#     ssl_context = ssl_context_configurator.create_context(True, ssl.CERT_REQUIRED)
#     connector = TCPConnector(ssl=ssl_context)
#     logger.info("Starting extraction process for CONFEF.")
#     scraper_confef = ScraperConfef(Variables.URL_CONFEF, bucket)
#     asyncio.run(scraper_confef.scraper(connector))
#     stopwatch.stop()
#     stopwatch.show_elapsed_time()
