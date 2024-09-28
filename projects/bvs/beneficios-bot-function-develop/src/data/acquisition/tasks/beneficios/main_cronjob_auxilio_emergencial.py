#!/usr/bin/env python
# -*- coding: utf-8 -*-


# ================================================================================================
# Module: main_cronjob_auxilio_emergencial.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data related to Auxílio Emergencial payments
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


from file_validation import FileValidator, ZipfileValidator
from file_unpacking import FileUnpacker
from file_downloader import UrllibDownloader, SSLContextConfigurator
from storage import GcsClient, GcsHandle, GcsFileValidator
from date_utils import CurrentDate, MonthlyDateHandle
from utils import EnvironmentUtils, Stopwatch
from variables import Variables
from app_log import AppLogging
import os
import sys
import ssl
import concurrent.futures

# Adding python modules from bucket (e.g. da-beneficios-jobs) to path
# These files are those passed in the "python_file_uris" parameter in "main.py"
sys.path.append(os.path.abspath("../"))


class ProcessRunner:
    """
    Class for running processes.
    """

    def __init__(
        self, project_id, bucket_name, local_dir, files, target_dates, logger
    ) -> None:
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.local_dir = local_dir
        self.files = files
        self.target_date = target_dates[0]
        self.target_date_name = target_dates[1]
        self.logger = logger

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
        url = f"https://www.portaldatransparencia.gov.br/download-de-dados/auxilio-emergencial/{self.target_date}"
        return url

    def get_download_path(self):
        """
        Get download path for a target date.
        """
        download_path = os.path.join(
            self.local_dir, f"auxilio_emergencial{self.target_date}.zip"
        )
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
        prefix = "AUXEMERGENCIAL/ENTRADA"
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
            check_hostname=True, verify_mode=ssl.CERT_REQUIRED
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

    def get_file_blob_map(self):
        """
        Get dictionary with blobs to send files.
        """
        file_blob_map = {
            f"{self.target_date}_AuxilioEmergencial.csv": "AUXEMERGENCIAL/ENTRADA/"
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

    def run(self):
        """
        Runs the process.
        """
        bucket = self.instantiate_bucket()
        url = self.get_url()
        zipfile_path = self.get_download_path()
        file_names = self.get_file_names()
        list_blobs = self.get_blobs_to_validate(bucket)
        file_exists_in_bucket = self.checking_if_file_exists_in_gcs(
            list_blobs, file_names
        )
        if not file_exists_in_bucket:
            self.logger.info(
                f"Starting the download for the Auxílio Emergencial social benefit from {self.target_date_name}."
            )
            context = self.set_ssl_context()
            self.download_file(url, context, zipfile_path)
            # Change the directory of "dataproc job" to "/tmp"
            os.chdir("../")
            file_exists = self.validate_if_file_exists(zipfile_path)
            zipfile_is_valid = self.validate_if_zipfile_is_valid(zipfile_path)
            if zipfile_is_valid and file_exists:
                self.unzip_files(zipfile_path)
                file_blob_map = self.get_file_blob_map()
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

    files = ["AuxilioEmergencial.csv"]

    logger.info(f"{'*' * 50} TASK STARTED {'*' * 50}")

    # Setting parameters for the monthly
    periods_to_check = 6
    current_date = CurrentDate()
    today = current_date.get_today()
    monthly_date_handle = MonthlyDateHandle(today)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for period in range(periods_to_check):
            target_dates = [
                monthly_date_handle.get_target_month_date(period, "%Y%m"),
                monthly_date_handle.get_target_month_date(period, "%B %Y"),
            ]
            process_runner = ProcessRunner(
                PROJECT_ID,
                BUCKET_NAME,
                TMP_DIR,
                files,
                target_dates,
                logger,
            )
            futures.append(executor.submit(process_runner.run))

    for _ in concurrent.futures.as_completed(futures):
        continue

    logger.info(f"{'*' * 50} TASK FINISHED {'*' * 50}")

    stopwatch.stop()
    stopwatch.elapsed_time(logger)


if __name__ == "__main__":
    main()
