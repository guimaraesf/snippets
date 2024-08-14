# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: conselho_federal_fonoaudiologia.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for scraping data from the Conselho Federal de Fonoaudiologia
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import asyncio
import importlib
import os
import random
import re
import sys
from urllib.error import HTTPError

import nest_asyncio
from google.cloud import storage
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.webdriver import WebDriver as ChromeDriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# When run in Dataproc workspace the original directory structure is not preserved.
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


COLUMNS_MOD_PATH = get_module_path("src.utils.dataframe.pyspark.schema.columns")
GCS_CLIENT_MOD_PATH = get_module_path("src.utils.gcp.storage.gcs_client")
GCS_MANAGER_MOD_PATH = get_module_path("src.utils.gcp.storage.gcs_manager")
LOGGER_MOD_PATH = get_module_path("src.utils.helpers.logger")
PANDAS_FILE_HANDLE = get_module_path("src.utils.dataframe.pandas.file_handler")
UTILS_MOD_PATH = get_module_path("src.utils.helpers.utils")
UTILS_SELENIUM_MOD_PATH = get_module_path("src.utils.web_tools.utils_selenium")
ISCRAPER_MOD_PATH = get_module_path("src.interfaces.interface_scraper")
STOPWATCH_MOD_PATH = get_module_path("src.utils.time.stopwatch")
VARIABLES_MOD_PATH = get_module_path("src.utils.helpers.variables")

Columns = importlib.import_module(COLUMNS_MOD_PATH).Columns
GcsClient = importlib.import_module(GCS_CLIENT_MOD_PATH).GcsClient
GcsManager = importlib.import_module(GCS_MANAGER_MOD_PATH).GcsManager
Logger = importlib.import_module(LOGGER_MOD_PATH).Logger
PandasFileHandle = importlib.import_module(PANDAS_FILE_HANDLE).PandasFileHandle
IScraper = importlib.import_module(ISCRAPER_MOD_PATH).IScraper
Stopwatch = importlib.import_module(STOPWATCH_MOD_PATH).Stopwatch
UtilsSelenium = importlib.import_module(UTILS_SELENIUM_MOD_PATH).UtilsSelenium
UtilsDataproc = importlib.import_module(UTILS_MOD_PATH).UtilsDataproc
Variables = importlib.import_module(VARIABLES_MOD_PATH).Variables


class ScraperCffa(IScraper):
    """
    Class responsible for scraping concrete Susep data.
    """

    def __init__(
        self,
        url: str,
        bucket_obj: storage.Bucket,
        logger_obj=Logger,
        gcs_manager=GcsManager,
        pandas_file_handle=PandasFileHandle,
        columns=Columns,
        variables=Variables,
        selenium=UtilsSelenium,
    ) -> None:
        """
        Initializes the ScraperCffa class.

        Args:
            url (str): The URL to scrape data from.
            bucket (storage.Bucket): The Google Cloud Storage bucket to use.
            logger (Logger, optional): The logger to use. Defaults to Logger.
            gcs_manager (GcsManager, optional): The Google Cloud Storage manager to use. Defaults to GcsManager.
            pandas_file_handle (PandasFileHandle, optional): The file handler to use. Defaults to PandasFileHandle.
            async_request (AsyncRequest, optional): The asynchronous request to use. Defaults to AsyncRequest.
            variables (Variables, optional): The variables to use. Defaults to Variables.
        """
        self.url = url
        self.logger = logger_obj()
        self.bucket = bucket_obj
        self.gcs_manager = gcs_manager(self.bucket)
        self.pandas_file_handle = pandas_file_handle()
        self.columns = columns
        self.variables = variables
        self.selenium = selenium(self.url)
        self.cluster_id = sys.argv[3]
        self.port = sys.argv[8]
        self.tmp_dir = sys.argv[9]

    def create_blob_staging_path(self, uf: str) -> str:
        """
        Create a blob path for a specific file in Google Cloud Storage.

        Args:
            uf (str): The unique identifier for the file.

        Returns:
            str: The blob path in Google Cloud Storage.
        """
        layer_name = self.variables.STAGING_NAME
        dir_name = self.variables.DIR_NAME_CFFA
        file_name = self.variables.FILENAME.format(uf.lower(), "cffa")
        blob_name = self.variables.BLOB_NAME.format(layer_name, dir_name, file_name)
        return blob_name

    def set_options_browser(self, browser_options: callable):
        """
        Configures the browser options for a Selenium WebDriver.

        Args:
            browser_options (callable): A function that returns a browser options object.

        Returns:
            options: A configured browser options object.
        """
        SUFFIX_MASTER_NODE = "m"
        args = (
            "--ignore-certificate-errors",
            "--headless",
            "--no-sandbox",
            "--disable-extensions",
            "--dns-prefetch-disable",
            "--disable-web-security",
            "--allow-insecure-localhost",
            f"user-agent={random.choice(self.variables.USER_AGENTS)}",  # nosec
            f"--user-data-dir={self.tmp_dir}/{self.cluster_id}-{SUFFIX_MASTER_NODE}",
        )
        options = self.selenium.instantiate_browser_options(browser_options)
        for arg in args:
            self.selenium.add_argument_option(options, arg)
        return options

    def extract_data_from_page(self, elements: list[str]) -> list[list]:
        """
        Extracts table data from HTML elements.

        Args:
            elements (str): A string of HTML elements.

        Returns:
            rows[list[list]]: A list of lists, where each inner list represents a row of data extracted from the HTML elements.
        """
        data, rows = ([], [])
        NUMBER_OF_COLUMNS, NO_BREAK_SPACE = (7, r"\xa0")
        for e in elements:
            if not re.search(NO_BREAK_SPACE, e.text):
                if len(data) == 0 and e.text.strip() == "":
                    continue
                data.append(e.text)
                if len(data) == NUMBER_OF_COLUMNS:
                    rows.append(data)
                    data = []
        return rows

    def navigate_and_extract_data(self, uf: str) -> list[str]:
        """
        Navigate to a specific page and extract data based on the provided state (uf).

        This method creates a new instance of Browser Options and Browser Driver, navigates to a specific page,
        selects a value from a dropdown menu, clicks a submit button, waits for a specific element to load,
        executes a script to get the inner HTML of an element, finds elements by tag name, and extracts data from the page.

        Args:
            uf (str): The state for which to extract data.

        Returns:
            list[str]: The extracted data from the page.

        Raises:
            HTTPError: If there is an HTTP error while navigating or extracting data.
        """
        # Create a new instance of Browser Options and add arguments
        options = self.set_options_browser(ChromeOptions)

        # Create a new instance of Browser Driver
        driver = self.selenium.instantiate_browser_driver(
            ChromeDriver, ChromeDriverManager, options
        )
        try:
            self.selenium.go_to_page(driver)
            element = self.selenium.find_element(driver, By.ID, "form-field-estado")
            self.selenium.select_by_value(element, uf)
            self.selenium.find_element(driver, By.CSS_SELECTOR, "button[type='submit']").click()
            self.selenium.webdrive_wait(driver, By.ID, "printTable")
            self.selenium.execute_script(
                driver, "return document.getElementById('printTable').innerHTML;"
            )
            elements = self.selenium.find_elements(driver, By.TAG_NAME, "td")

            data = self.extract_data_from_page(elements)
            if not data:
                self.logger.warning(f"The request from {uf} returned empty.")
                return None
            return data

        except HTTPError as e:
            self.logger.error(f"HTTP Error {e.status} - {e.reason}.")

        finally:
            self.selenium.close_browser(driver)

    def process_df_from_source(self, data: list, blob_name: str) -> None:
        """
        Processes a DataFrame from a source object and saves it to a CSV file.

        Args:
            data (dict): The data to be processed.
            blob_name (str): The path where the file will be saved in bucket.
        """
        self.logger.info("Reading as DataFrame from Web Page.")
        columns_name = list(self.columns.TABLE_INFOS_CFFA.keys())
        df = self.pandas_file_handle.create_df(data, columns_name)

        self.logger.info(f"Total number of records: {df.shape[0]}")
        csv_data = self.pandas_file_handle.convert_to_csv_file(df=df)

        self.logger.info("Uploading CSV files in the bucket.")
        blob = self.gcs_manager.create_blob(blob_name)
        self.gcs_manager.blob_upload_from_string(blob, csv_data)
        self.logger.info(f"File were uploaded successfully to GCS: {blob.name}")

    async def process_response(self, uf: str) -> None:
        """
        Processes the response for a given session and state.

        Args:
            uf (str): The state to process the response for.
        """
        self.logger.info(f"Starting web page navigation from {uf}.")
        data = self.navigate_and_extract_data(uf)
        self.logger.info("The browser has been closed.")
        blob_name = self.create_blob_staging_path(uf)
        self.process_df_from_source(data, blob_name)
        await asyncio.sleep(1)

    async def scraper(self) -> None:
        """
        Main method that runs all tasks for all states.
        """
        tasks = [self.process_response(uf) for uf in self.variables.UF]
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

    logger.info("Starting extraction process for CFFA.")
    scraper_cffa = ScraperCffa(Variables.URL_CFFA, bucket)
    asyncio.run(scraper_cffa.scraper())

    stopwatch.stop()
    stopwatch.show_elapsed_time()
