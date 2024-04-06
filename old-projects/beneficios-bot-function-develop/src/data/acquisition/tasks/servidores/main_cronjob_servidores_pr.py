#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: main_cronjob_servidores_pr.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data related of Public Servants (PR)
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


from file_processing import PandasCsvHandle
from file_validation import FileValidator, ZipfileValidator
from file_unpacking import FileUnpacker
from file_downloader import UrllibDownloader, SSLContextConfigurator
from storage import GcsClient, GcsHandle, GcsFileValidator
from date_utils import CurrentDate, DailyDateHandle
from utils import EnvironmentUtils, Stopwatch
from variables import Variables
from app_log import AppLogging
import os
import sys
import ssl
# Adding python modules from bucket (e.g. da-beneficios-jobs) to path
# These files are those passed in the "python_file_uris" parameter in "main.py"
sys.path.append(os.path.abspath("../"))


class ProcessRunner:
    """
    Class for running processes.
    """

    def __init__(self, project_id, bucket_name, local_dir, files, target_date, logger) -> None:
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.local_dir = local_dir
        self.files = files
        self.target_date = target_date
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
        url = "http://www.transparencia.download.pr.gov.br/exportacao/REMUNERACAO_RH/REMUNERACAO_RH.zip?windowId=42d"
        return url

    def get_download_path(self):
        """
        Get download path for a target date.
        """
        download_path = os.path.join(self.local_dir, "REMUNERACAO_RH.zip")
        return download_path

    def get_file_names(self):
        """
        Returns a list of file names for the target date.
        """
        file_names = [f"{self.target_date}_{file_name}" for file_name in self.files]
        return file_names

    def get_blobs_to_validate(self, bucket):
        """
        Listing existing blobs based on prefix.
        """
        prefix = "SERVIDORES/ESTADUAIS/PR/ENTRADA"
        gcs_handle = GcsHandle(bucket, self.logger)
        list_blobs = gcs_handle.get_list_blobs(prefix)
        return list_blobs

    def checking_if_file_exists_in_gcs(self, list_blobs, file_names):
        """
        Checking if the file exists in Google Cloud Storage.
        """
        gcs_file_validator = GcsFileValidator()
        file_exists = gcs_file_validator.file_exists_in_bucket(list_blobs, file_names, self.logger)
        return file_exists

    def set_ssl_context(self):
        """
        Configures the SSL context.
        """
        ssl_configurator = SSLContextConfigurator(
            check_hostname=False, verify_mode=ssl.CERT_NONE
        )
        context = ssl_configurator.configure_ssl_context()
        return context

    def download_file(self, url, context, zipfile_path):
        """
        Running download files.
        """
        file_downloader = UrllibDownloader(
            url, context, zipfile_path, self.logger
        )
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
        file_unpacker = FileUnpacker(
            zipfile_path, self.local_dir, self.logger
        )
        file_unpacker.unpack_zipfile()

    def get_file_blob_map(self):
        """
        Get dictionary with blobs to send files.
        """
        file_blob_map = {
            f"{self.target_date}_Servidores_PR.csv": "SERVIDORES/ESTADUAIS/PR/ENTRADA/"
        }
        return file_blob_map

    def get_new_file_name(self):
        """
        Returns the new file name.
        """
        new_file_name = f"{self.target_date}_Servidores_PR.csv"
        return new_file_name

    def upload_files_to_gcs(self, bucket, file_names_to_send, file_blob_map):
        """
        Uploading files into Google Cloud Storage.
        """
        gcs_handle = GcsHandle(bucket, self.logger)
        gcs_handle.upload_blobs_to_gcs(self.local_dir, file_names_to_send, file_blob_map)

    def read_csv_file(self, file_path):
        """
        Read file on Pandas DataFrame.
        """
        dataframe = self.pd_csv_handle.read_csv_file(
            file_path, sep=";", encoding="UTF-8"
        )
        return dataframe

    def dataframe_to_csv(self, dataframe, file_path):
        """
        Convert dataframe to CSV format.
        """
        self.pd_csv_handle.convert_to_csv_file(
            dataframe, file_path, sep=";", encoding="ISO-8859-1"
        )
        self.logger.info(f"File successfully saved as csv to {file_path}.")

    def deleting_files_in_gcs(self, bucket, file_name, list_blobs):
        """
        Deleting files stored at earlier dates.
        """
        gcs_handle = GcsHandle(bucket, self.logger)
        for blob in list_blobs:
            if blob.exists() and file_name not in blob.name:
                gcs_handle.delete_blob(blob)
                self.logger.info(f"Deleting blob at earlier dates: {blob.name}")

    def run(self):
        """
        Runs the process.
        """
        bucket = self.instantiate_bucket()
        url = self.get_url()
        zipfile_path = self.get_download_path()
        file_names = self.get_file_names()
        list_blobs = self.get_blobs_to_validate(bucket)
        self.logger.info("Starting the download for the Public Servants (PR).")
        context = self.set_ssl_context()
        self.download_file(url, context, zipfile_path)
        # Change the directory of "dataproc job" to "/tmp"
        os.chdir("../")
        file_exists = self.validate_if_file_exists(zipfile_path)
        zipfile_is_valid = self.validate_if_zipfile_is_valid(zipfile_path)
        if zipfile_is_valid and file_exists:
            self.unzip_files(zipfile_path)
            file_blob_map = self.get_file_blob_map()
            new_file_name = self.get_new_file_name()
            old_file_path = os.path.join(self.local_dir, "TB_RH.csv")
            new_file_path = os.path.join(self.local_dir, new_file_name)
            if os.path.exists(old_file_path):
                os.renames(old_file_path, new_file_path)
                dataframe = self.read_csv_file(new_file_path)
                self.dataframe_to_csv(dataframe, new_file_path)
                self.deleting_files_in_gcs(bucket, new_file_name, list_blobs)
                self.upload_files_to_gcs(bucket, file_names, file_blob_map)


def main():
    """
    Main function.
    """
    stopwatch = Stopwatch()
    stopwatch.start()

    # Getting all environment variables for dataproc workflow template
    env_utils = EnvironmentUtils()
    workflow_env_vars: dict = env_utils.get_args(Variables.ACQUISITION_ARGS)

    TMP_DIR = workflow_env_vars.get("tmp_dir")
    PROJECT_ID = workflow_env_vars.get("project_id")
    BUCKET_NAME = workflow_env_vars.get("bucket_name")

    logger = AppLogging()

    files = ["Servidores_PR.csv"]

    logger.info(f"{'*' * 50} TASK STARTED {'*' * 50}")

    # Setting parameters for the monthly
    current_date = CurrentDate()
    today = current_date.get_today()
    daily_date_handle = DailyDateHandle(today)
    target_date = daily_date_handle.get_target_day_date(0, "%Y%m%d")

    process_runner = ProcessRunner(
        PROJECT_ID,
        BUCKET_NAME,
        TMP_DIR,
        files,
        target_date,
        logger
    )
    process_runner.run()

    logger.info(f"{'*' * 50} TASK FINISHED {'*' * 50}")

    stopwatch.stop()
    stopwatch.elapsed_time(logger)


if __name__ == "__main__":
    main()
