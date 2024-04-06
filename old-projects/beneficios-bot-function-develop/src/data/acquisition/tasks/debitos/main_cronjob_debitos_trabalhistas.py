#!/usr/bin/env python
# -*- coding: utf-8 -*-


# ================================================================================================
# Module: main_cronjob_debitos_trabalhistas.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data related to Débitos Trabalhistas
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
import logging
import zipfile
import ssl
import urllib.request
import urllib.error
import re
from datetime import date
from abc import ABC, abstractmethod
from google.cloud import storage
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup


class Validator(ABC):
    """
    Abstract class for validators.
    """

    @abstractmethod
    def validate(self):
        """
        Validates the data.
        """
        pass


class GcsClient:
    """
    This class instantiates the Google Cloud Storage client and the GCP bucket.
    """

    def __init__(self, project_id, bucket_id, logging):
        self.project_id = project_id
        self.bucket_id = bucket_id
        self.logging = logging

    def __instantiating_client(self):
        """
        Instantiates the Google Cloud Storage client.
        :return: The Google Cloud Storage client.
        """
        return storage.Client(self.project_id)

    def create_bucket_object(self):
        """
        Instantiates the GCP bucket.
        :param client: The Google Cloud Storage client.
        :return: The GCP bucket.
        """
        self.logging.info(
            f"Configuring the application to access the GCP bucket: {self.bucket_id}"
        )
        client = self.__instantiating_client()
        return client.bucket(self.bucket_id)


class GcsSender:
    """
    Class for sending files to GCS.
    """

    def __init__(
        self, bucket, source_path, file_name, target_date, logging, regex_operations=re
    ):
        self.bucket = bucket
        self.source_path = source_path
        self.file_name = file_name
        self.target_date = target_date
        self.logging = logging
        self.regex_operations = regex_operations

    @staticmethod
    def get_file_blob_map():
        """
        Returns a map of blob names to blob objects.
        :return: A map of blob names to blob objects.
        """
        return {
            "Previdenciario": "DEBITOSTRABALHISTAS/PREVIDENCIARIO/ENTRADA",
            "Nao_Previdenciario": "DEBITOSTRABALHISTAS/NAOPREVIDENCIARIO/ENTRADA",
            "FGTS": "DEBITOSTRABALHISTAS/FGTS/ENTRADA",
        }

    @staticmethod
    def process_blob(blob, file_path):
        """
        Processes a blob.
        :param blob: The blob object.
        :param file_path: The file path.
        """
        with open(file_path, "rb") as csv_file:
            blob.upload_from_file(csv_file, content_type="application/zip")

    def get_pattern(self, regex_expression):
        """
        Returns a regex pattern.
        :param regex_expression: The regex expression.
        :return: A regex pattern.
        """
        return self.regex_operations.compile(regex_expression)

    @staticmethod
    def get_pattern_matching(file_name, pattern):
        """
        Returns a regex pattern matching.
        :param file_name: The file name.
        :param pattern: The regex pattern.
        :return: A regex pattern matching.
        """
        return pattern.match(file_name)

    @staticmethod
    def get_list_keys_blob_map(blob_map):
        """
        Returns a list of blob names.
        :param blob_map: The blob map.
        :return: A list of blob names.
        """
        return list(blob_map.keys())

    def is_valid_match(self, file_name, tag_name, keys_blob_map):
        """
        Checks if the file name is valid.
        :param file_name: The file name.
        :param tag_name: The tag name.
        :param keys_blob_map: The list of blob names.
        :return: True if the file name is valid, False otherwise.
        """
        if not file_name.startswith(self.target_date):
            return False

        if tag_name not in file_name:
            return False

        if tag_name not in keys_blob_map:
            return False

        return True

    def upload_to_gcs(self, blob_map, regex_expression):
        """
        Uploads files to GCS.
        :param blob_map: The blob map.
        :param regex_expression: The regex expression.
        """
        try:
            self.logging.info("Uploading files to GCS.")
            keys_blob_map = self.get_list_keys_blob_map(blob_map)
            pattern = self.get_pattern(regex_expression)
            for file in os.listdir(self.source_path):
                match = self.get_pattern_matching(file, pattern)
                if match:
                    tag_name = match.group(2)
                    if self.is_valid_match(file, tag_name, keys_blob_map):
                        blob = self.bucket.blob(os.path.join(blob_map[tag_name], file))
                        self.process_blob(blob, os.path.join(self.source_path, file))
            self.logging.info("All files were uploaded successfully.")

        except FileNotFoundError:
            self.logging.error("File not found.")

        except Exception as exception:
            self.logging.error(f"Error uploading files to GCP: {exception}")


class GcsFileValidator:
    """
    Validator for storage file validity.
    """

    def __init__(self, bucket, pattern, target_date, regex_operations=re):
        self.bucket = bucket
        self.pattern = pattern
        self.target_date = target_date
        self.regex_operations = regex_operations

    def get_list_blobs(self):
        """
        Returns a list of blob objects.
        :return: A list of blob objects.
        """
        return list(self.bucket.list_blobs())

    def validate_file_existence(self, list_blobs):
        """
        Checks if the files exist.
        :param list_blobs: The list of blob objects.
        :return: True if the files exist, False otherwise.
        """
        for blob in list_blobs:
            csv_file = blob.name.split("/")[-1]
            match = self.regex_operations.match(self.pattern, csv_file)
            if match:
                tag_date = match.group(1)
                if tag_date == self.target_date:
                    return True
        return False


class FileValidator(Validator):
    """
    Validator for file existence.
    """

    def __init__(self, file_path, logging):
        self.file_path = file_path
        self.logging = logging

    def validate(self):
        """
        Checks if the file exists.
        :return: True if the file exists, False otherwise.
        """
        try:
            if os.path.exists(self.file_path):
                return True

        except FileNotFoundError as exception:
            self.logging.error(f"File not found: {exception}")


class ZipfileValidator(Validator):
    """
    Validator for zipfile validity.
    """

    def __init__(self, zipfile_path, logging):
        self.zipfile_path = zipfile_path
        self.logging = logging

    def validate(self):
        """
        Checks if the zipfile is valid.
        :return: True if the zipfile is valid, False otherwise.
        """
        try:
            if zipfile.is_zipfile(self.zipfile_path):
                return True

        except zipfile.BadZipFile as exception:
            self.logging.error(f"Failed to extract zip file: {exception}")


class FileUnpacker:
    """
    Class for unpacking files.
    """

    def __init__(self, file_name, target_date, source_path, extract_path, logging):
        self.file_name = file_name
        self.target_date = target_date
        self.source_path = source_path
        self.extract_path = extract_path
        self.logging = logging

    def rename_files(self, file_info, tag_id, ext):
        """
        Renames the files.
        :param file_info: The file info.
        :param tag_id: The tag id.
        :param ext: The file extension.
        """
        new_file_name = (
            f"{self.target_date}_{self.file_name[14:-4]}_{str(tag_id).zfill(3)}{ext}"
        )
        old_file_path = os.path.join(self.extract_path, file_info.filename)
        new_file_path = os.path.join(self.extract_path, new_file_name)
        os.rename(old_file_path, new_file_path)

    def unpack_zipfile(self):
        """
        Opens and extracts the files.
        """
        with zipfile.ZipFile(self.source_path, "r") as zip_ref:
            tag_id = 0
            for file_info in zip_ref.infolist():
                filename, ext = os.path.splitext(file_info.filename)
                zip_ref.extract(file_info.filename, path=self.extract_path)
                self.rename_files(file_info, tag_id, ext)
                tag_id += 1
            self.logging.info(
                f"All files extracted and processed from {self.source_path} to {self.extract_path}."
            )


class DateFinder:
    """
    Class for finding dates.
    """

    @staticmethod
    def get_today():
        """
        Returns the current date.
        """
        return date.today()

    @staticmethod
    def get_current_year(today) -> int:
        """
        Get the current year in the format "yyyy".

        Returns:
            str: The current year in the format "yyyy".
        """
        current_year = today.year
        return current_year

    @staticmethod
    def get_quarter_number(today) -> int:
        """
        Get the number of the current quarter.

        Returns:
            int: The number of the current quarter (1, 2, 3 or 4).
        """
        quarter_start_month = (today.month - 1) // 3 * 3 + 1
        quarter_number = (quarter_start_month - 1) // 3 + 1
        return quarter_number

    @staticmethod
    def get_previous_quarter(current_year, current_quarter, index):
        """
        Get the previous quarter.
        """
        target_date = date(
            int(current_year), (int(current_quarter) - 1) * 3 + 1, 1
        ) - relativedelta(months=index * 3)
        target_year = target_date.year
        target_quarter = (target_date.month - 1) // 3 + 1
        return f"{target_year}{target_quarter:02}"


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
        context = ssl.create_default_context()
        context.check_hostname = self.check_hostname
        context.verify_mode = self.verify_mode
        return context


class FileDownloader:
    """
    Class for downloading files.
    """

    def __init__(
        self,
        file_to_download,
        context,
        target_date,
        path_where_to_save,
        logging,
        urlopen=urllib.request.urlopen,
        beutiful_soup=BeautifulSoup,
    ):
        self.file_to_download = file_to_download
        self.context = context
        self.target_date = target_date
        self.path_where_to_save = path_where_to_save
        self.logging = logging
        self.urlopen = urlopen
        self.beutiful_soup = beutiful_soup

    def execute_request(self, url):
        """
        Executes the request.
        """
        try:
            return self.urlopen(url, context=self.context)
        except urllib.error.HTTPError as exception:
            self.logging.error(
                f"The request was not successful: HTTP Error {exception.code}: {exception.reason}"
            )

    def parse_url(self, html):
        """
        Parses the url.
        """
        return self.beutiful_soup(html.read(), "html.parser")

    @staticmethod
    def find_all_elements(soup):
        """
        Finds all the elements.
        """
        return soup.find_all("a")

    @staticmethod
    def extract_name_from_url(url, start_index, end_index, additional_string=None):
        """
        Extracts the name from the url.
        """
        snippet_name = "_".join(url.split("/")[start_index:end_index])
        if additional_string is not None:
            snippet_name += "_" + additional_string
        return snippet_name

    @staticmethod
    def is_valid_href(
        href, file_name_from_href, file_name_to_compare, date_from_href, date_to_compare
    ):
        """
        Checks if the href is valid.
        """
        if not href.startswith("https://"):
            return False

        if len(file_name_from_href) != len(file_name_to_compare):
            return False

        if date_to_compare != date_from_href:
            return False

        return True

    def extract_link(self, links):
        """
        Extracts the link.
        """
        valid_href = []
        for link in links:
            href = link["href"]
            year = self.target_date[:4]
            quarter = self.target_date[4:6]

            date_from_href = self.extract_name_from_url(href, 3, 4)
            date_to_compare = "_".join([year, "trimestre", quarter])

            file_name_from_href = self.extract_name_from_url(href, 3, 5)
            file_name_to_compare = self.extract_name_from_url(
                href, 3, 4, self.file_to_download
            )

            if self.is_valid_href(
                href,
                file_name_from_href,
                file_name_to_compare,
                date_from_href,
                date_to_compare,
            ):
                valid_href.append(href)

        return valid_href

    @staticmethod
    def get_status_code(response):
        """
        Get the status code.
        """
        return response.status

    @staticmethod
    def get_reason(response):
        """
        Get the reason.
        """
        return response.reason

    def process_response(self, response, file_name):
        """
        Processes the response.
        """
        logging.info(f"Downloading file to: {self.path_where_to_save}")
        file_path = os.path.join(
            self.path_where_to_save, f"{self.target_date}_{file_name}"
        )
        with open(file_path, "wb") as outfile:
            outfile.write(response)
            self.logging.info(f"File {file_path} was downloaded successfully.")

    def verifying_request(self, response, status_code, reason, file_name):
        """
        Verifies the request.
        """
        self.logging.info("Verifying that the request was successful.")
        if status_code == 200:
            self.logging.info(
                f"The request was successful: HTTP {status_code}-{reason}"
            )
            self.process_response(response.read(), file_name)
        else:
            self.logging.error(
                f"The request was not successful: HTTP Error {status_code}-{reason}"
            )

    def download_file(self, urls):
        """
        Downloads the file.
        """
        for url in urls:
            file_name = url.split("/")[-1]
            response = self.execute_request(url)
            if response is not None:
                status_code = self.get_status_code(response)
                reason = self.get_reason(response)
                self.verifying_request(response, status_code, reason, file_name)


class ProcessRunner:
    """
    Class for running processes.
    """

    def __init__(
        self, url, file, target_date, bucket, local_dir, regex_expression, logging
    ) -> None:
        self.url = url
        self.file = file
        self.target_date = target_date
        self.bucket = bucket
        self.local_dir = local_dir
        self.regex_expression = regex_expression
        self.logging = logging

    def run(self):
        """
        Runs the process.
        """
        # Configures the SSL context.
        ssl_configurator = SSLContextConfigurator(
            check_hostname=False, verify_mode=ssl.CERT_NONE
        )
        context = ssl_configurator.configure_ssl_context()

        # Download files
        file_downloader = FileDownloader(
            self.file, context, self.target_date, self.local_dir, logging
        )

        # Open the URL and read its contents
        html = file_downloader.execute_request(self.url)

        # Parse the HTML content using BeautifulSoup
        soup = file_downloader.parse_url(html)

        # Find all elements with the class
        links = file_downloader.find_all_elements(soup)

        # Loop through each element and extract its "href" attribute
        href = file_downloader.extract_link(links)

        # Running download
        file_downloader.download_file(href)

        # Unpacking files to "/tmp"
        os.chdir("../")
        zipfile_path = os.path.join(self.local_dir, f"{self.target_date}_{self.file}")

        # Checking if the file exists
        file_validator = FileValidator(zipfile_path, self.logging)
        file_exists = file_validator.validate()

        # Checking if the zipfile is valid
        zipfile_validator = ZipfileValidator(zipfile_path, self.logging)
        zipfile_is_valid = zipfile_validator.validate()

        if zipfile_is_valid and file_exists:
            # Extract all files
            file_unpacker = FileUnpacker(
                self.file, self.target_date, zipfile_path, self.local_dir, self.logging
            )
            file_unpacker.unpack_zipfile()

            # Uploading files into Google Cloud Storage
            gcs_sender = GcsSender(
                self.bucket, self.local_dir, self.file, self.target_date, self.logging
            )
            file_blob_map = gcs_sender.get_file_blob_map()
            gcs_sender.upload_to_gcs(file_blob_map, self.regex_expression)


def main():
    """
    Main function.
    """
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S.%f"[:-3],
        level=logging.INFO,
    )

    logging.info("The cronjob has been started.")

    # Configure Client Cloud Storage
    PROJECT_ID = sys.argv[3]
    BUCKET_ID = sys.argv[2]
    TMP_DIR = sys.argv[3]
    gcs_client = GcsClient(PROJECT_ID, BUCKET_ID, logging)
    bucket = gcs_client.create_bucket_object()

    # Setting date parameters
    date_finder = DateFinder()
    today = date_finder.get_today()
    quarters_to_check = 2

    # Setting the regex expression that defines the file name pattern for identification
    regex_expression = r"(\d{6})_(FGTS|Nao_Previdenciario|Previdenciario)_(\d+)\.csv"
    url = "https://www.gov.br/pgfn/pt-br/assuntos/divida-ativa-da-uniao/transparencia-fiscal-1/copy_of_dados-abertos"
    files_to_download = (
        "Dados_abertos_Previdenciario.zip",
        "Dados_abertos_Nao_Previdenciario.zip",
        "Dados_abertos_FGTS.zip",
    )

    # Scroll through each of the quarter
    for quarter in range(quarters_to_check):
        current_year = date_finder.get_current_year(today)
        current_quarter = date_finder.get_quarter_number(today)
        target_date = date_finder.get_previous_quarter(
            current_year, current_quarter, quarter + 1
        )

        gcs_file_validator = GcsFileValidator(bucket, regex_expression, target_date)
        blobs = gcs_file_validator.get_list_blobs()
        if_file_exists = gcs_file_validator.validate_file_existence(blobs)

        if not if_file_exists:
            logging.info(f"Files from {target_date} dos not exists in the bucket.")
            for file in files_to_download:
                logging.info(
                    f"Starting the download for the Labor Debts - {file[14:-4]} from {target_date}."
                )
                process_runner = ProcessRunner(
                    url, file, target_date, bucket, TMP_DIR, regex_expression, logging
                )
                process_runner.run()
        else:
            logging.info(f"Files from {target_date} already exists in the bucket.")

    logging.info("Cronjob successfully finished.")


if __name__ == "__main__":
    main()
