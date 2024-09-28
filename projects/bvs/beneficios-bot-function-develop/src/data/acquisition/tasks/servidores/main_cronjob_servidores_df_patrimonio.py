#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# ================================================================================================
# Module: main_cronjob_servidores_df.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data on remuneration of Public Servants (DF)
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import ssl
import sys

# Adding python modules from bucket (e.g. da-beneficios-jobs) to path
# These files are those passed in the "python_file_uris" parameter in "main.py"
sys.path.append(os.path.abspath("../"))
from file_processing import PandasCsvHandle
from file_validation import FileValidator, ZipfileValidator
from file_unpacking import FileUnpacker
from file_downloader import UrllibDownloader, SSLContextConfigurator
from storage import GcsClient, GcsHandle, GcsFileValidator
from date_utils import CurrentDate
from utils import EnvironmentUtils, Stopwatch
from variables import Variables
from app_log import AppLogging


class ProcessRunner:
    """
    Class for running processes.
    """

    def __init__(
        self, project_id, bucket_name, local_dir, files, dates_for_blobs, logger
    ) -> None:
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.local_dir = local_dir
        self.files = files
        self.dates_for_blobs = dates_for_blobs
        self.logger = logger
        self.pd_csv_handle = PandasCsvHandle(self.logger)

    def instantiate_bucket(self):
        """
        Configure Client Cloud Storage.
        """
        gcs_client = GcsClient(self.project_id, self.bucket_name, self.logger)
        bucket = gcs_client.create_bucket_object()
        return bucket

    def get_url(self):
        """
        Returns the URL for the specified target date.
        """
        url = "https://www.transparencia.df.gov.br/arquivos/Patrimonio_Bens_Moveis.zip"
        return url

    def get_download_path(self):
        """
        Get download path for a target date.
        """
        download_path = os.path.join(self.local_dir, "Patrimonio_Bens_Moveis.zip")
        return download_path

    def get_blobs_to_validate(self, bucket):
        """
        Listing existing blobs based on prefix.
        """
        prefix = "SERVIDORES/ESTADUAIS/STAGING/DF/PATRIMONIO"
        gcs_handle = GcsHandle(bucket, self.logger)
        list_blobs = gcs_handle.get_list_blobs(prefix)
        return list_blobs

    def checking_if_file_exists_in_gcs(self, list_blobs, file_names):
        """
        Checking if the file exists in Google Cloud Storage.
        """
        gcs_file_validator = GcsFileValidator()
        file_exists = gcs_file_validator.file_exists_in_bucket(
            list_blobs, file_names, self.logger
        )
        return file_exists

    def set_ssl_context(self):
        """
        Configures the SSL context.
        """
        ssl_configurator = SSLContextConfigurator(
            check_hostname=None, verify_mode=ssl.CERT_NONE
        )
        context = ssl_configurator.configure_ssl_context()
        return context

    def download_file(self, url, context, zipfile_path):
        """
        Running download files.
        """
        file_downloader = UrllibDownloader(url, context, zipfile_path, self.logger)
        file_downloader.run_download()

    def validate_if_file_exists(self, zipfile_path):
        """
        Checking if the file exists in "/tmp".
        """
        file_validator = FileValidator(zipfile_path, self.logger)
        file_exists = file_validator.validate()
        return file_exists

    def validate_if_zipfile_is_valid(self, zipfile_path):
        """
        Checking if the zipfile is valid.
        """
        zipfile_validator = ZipfileValidator(zipfile_path, self.logger)
        zipfile_is_valid = zipfile_validator.validate()
        return zipfile_is_valid

    def unzip_files(self, zipfile_path):
        """
        Unpacking files to "/tmp".
        """
        file_unpacker = FileUnpacker(zipfile_path, self.local_dir, self.logger)
        file_unpacker.unpack_zipfile()

    def get_file_blob_map(self, structure_dir):
        """
        Get dictionary with blobs to send files.
        """
        file_blob_map = {
            "Patrimonio_Bens_Moveis.csv": f"SERVIDORES/ESTADUAIS/STAGING/DF/PATRIMONIO/{structure_dir}",
        }
        return file_blob_map

    def upload_files_to_gcs(self, bucket, file_names_to_send, file_blob_map):
        """
        Uploading files into Google Cloud Storage.
        """
        gcs_handle = GcsHandle(bucket, self.logger)
        gcs_handle.upload_blobs_to_gcs(
            self.local_dir, file_names_to_send, file_blob_map
        )

    def deleting_files_in_gcs(self, bucket, file_name, list_blobs):
        """
        Deleting files stored at earlier dates.
        """
        gcs_handle = GcsHandle(bucket, self.logger)
        for blob in list_blobs:
            if blob.exists() and file_name[0] in blob.name:
                gcs_handle.delete_blob(blob)
                self.logger.warning("Deleting blob at earlier dates.")

    def read_csv_file(self, file_path):
        self.logger.info("Reading CSV file.")
        with open(file_path, "r", encoding="ISO-8859-1") as csv_file:
            first_line = csv_file.readline()
            rows = csv_file.read().split("\n")
        num_columns = first_line.count(";")
        return rows, num_columns

    def fix_rows(self, rows, num_columns):
        corrected_rows = []
        current_row = ""
        self.logger.info("Correcting unwanted line breaks in the file")
        for row in rows:
            if row.count(";") < num_columns:
                current_row += row
            else:
                corrected_rows.append(current_row)
                current_row = row
        return corrected_rows

    def save_csv_file(self, file_path, corrected_rows):
        self.logger.info("Writing file fixed.")
        with open(file_path, "w", encoding="ISO-8859-1") as csv_file:
            for line in corrected_rows:
                csv_file.write(line + "\n")

    def read_df_from_csv_file(self, file_path):
        """
        Read file on Pandas DataFrame.
        """
        dataframe = self.pd_csv_handle.read_csv_file(
            file_path,
            sep=";",
            encoding="ISO-8859-1",
            engine="python",
            on_bad_lines="skip",
        )
        return dataframe

    def dataframe_to_csv(self, dataframe, file_path):
        """
        Convert dataframe to CSV format.
        """
        self.pd_csv_handle.convert_to_csv_file(
            dataframe, file_path, sep=";", encoding="ISO-8859-1"
        )

    def run(self):
        """
        Runs the process.
        """
        year, month, day = self.dates_for_blobs
        bucket = self.instantiate_bucket()
        url = self.get_url()
        list_blobs = self.get_blobs_to_validate(bucket)
        zipfile_path = self.get_download_path()
        self.logger.info(
            "Starting the download for the Public Servants (DF) - Patrimônio Bens Imóveis."
        )
        context = self.set_ssl_context()
        self.download_file(url, context, zipfile_path)
        # Change the directory of "dataproc job" to "/tmp"
        os.chdir("../")
        file_exists = self.validate_if_file_exists(zipfile_path)
        zipfile_is_valid = self.validate_if_zipfile_is_valid(zipfile_path)
        if zipfile_is_valid and file_exists:
            self.unzip_files(zipfile_path)
            file_path = os.path.join(self.local_dir, self.files[0])
            rows, num_columns = self.read_csv_file(file_path)
            corrected_rows = self.fix_rows(rows, num_columns)
            self.save_csv_file(file_path, corrected_rows)
            dataframe = self.read_df_from_csv_file(file_path)
            self.dataframe_to_csv(dataframe, file_path)
            structure_dir = f"ANO={year}/MES={month}/DIA={day}/"
            file_blob_map = self.get_file_blob_map(structure_dir)
            self.deleting_files_in_gcs(bucket, self.files, list_blobs)
            self.upload_files_to_gcs(bucket, self.files, file_blob_map)


def main():
    """
    Main function.
    """
    stopwatch = Stopwatch()
    stopwatch.start()

    # Getting all environment variables for dataproc workflow template
    env_utils = EnvironmentUtils()
    workflow_env_vars: dict = env_utils.get_args(Variables.ACQUISITION_ARGS)

    # Setting temporary directory
    TMP_DIR = workflow_env_vars.get("tmp_dir")
    PROJECT_ID = workflow_env_vars.get("project_id")
    BUCKET_NAME = workflow_env_vars.get("bucket_name")

    logger = AppLogging()

    files = ["Patrimonio_Bens_Moveis.csv"]

    logger.info(f"{'*' * 50} TASK STARTED {'*' * 50}")

    current_date = CurrentDate()
    today = current_date.get_today()
    year, month, day = (today.strftime(fmt) for fmt in ["%Y", "%m", "%d"])
    dates_for_blobs = [year, month, day]

    process_runner = ProcessRunner(
        PROJECT_ID, BUCKET_NAME, TMP_DIR, files, dates_for_blobs, logger
    )
    process_runner.run()

    logger.info(f"{'*' * 50} TASK FINISHED {'*' * 50}")
    stopwatch.stop()
    stopwatch.elapsed_time(logger)


if __name__ == "__main__":
    main()
