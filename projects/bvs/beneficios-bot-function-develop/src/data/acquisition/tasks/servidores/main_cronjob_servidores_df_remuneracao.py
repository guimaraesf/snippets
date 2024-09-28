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
import shutil
import concurrent.futures

# Adding python modules from bucket (e.g. da-beneficios-jobs) to path
# These files are those passed in the "python_file_uris" parameter in "main.py"
sys.path.append(os.path.abspath("../"))
from file_processing import PandasCsvHandle
from file_validation import FileValidator, ZipfileValidator
from file_unpacking import FileUnpacker
from file_downloader import UrllibDownloader, SSLContextConfigurator
from storage import GcsClient, GcsHandle, GcsFileValidator
from date_utils import CurrentDate, MonthlyDateHandle, YearlyDateHandle
from utils import EnvironmentUtils, Stopwatch
from variables import Variables
from app_log import AppLogging


class ProcessRunner:
    """
    Class for running processes.
    """

    def __init__(
        self,
        project_id,
        bucket_name,
        local_dir,
        files,
        target_date,
        dates_for_blobs,
        logger,
    ) -> None:
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.local_dir = local_dir
        self.files = files
        self.target_date = target_date
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

    def get_url(self, target_date_year):
        """
        Returns the URL for the specified target date.
        """
        url = f"https://www.transparencia.df.gov.br/arquivos/Remuneracao_{target_date_year}.zip"
        return url

    def get_download_path(self, target_date_year):
        """
        Get download path for a target date.
        """
        download_path = os.path.join(
            self.local_dir, f"Remuneracao_{target_date_year}.zip"
        )
        return download_path

    def get_file_names(self):
        """
        Returns a list of file names for the target date.
        """
        # The interval indexes correspond to the 12 months of the year.
        file_names = [
            f"{self.target_date}{str(i).zfill(2)}_{file_name}"
            for i in range(1, 13)
            for file_name in self.files
        ]
        return file_names

    def get_blobs_to_validate(self, bucket):
        """
        Listing existing blobs based on prefix.
        """
        prefix = "SERVIDORES/ESTADUAIS/STAGING/DF/REMUNERACAO"
        gcs_handle = GcsHandle(bucket, self.logger)
        list_blobs = gcs_handle.get_list_blobs(prefix)
        return list_blobs

    def checking_if_file_exists_in_gcs(self, list_blobs, file_names, tag_name):
        """
        Checking if the file exists in Google Cloud Storage.
        """
        if self.target_date != tag_name:
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

    def get_date_prefix(self, file_name, splited_file_name):
        """
        Get the date prefix.
        """
        if "Detalhamento" in file_name:
            # e.g splited_file_name = ["Remuneracao", "Detalhamento", 2023", "MM.csv"]
            date_prefix = f"{splited_file_name[2]}{splited_file_name[3][:-4]}"
            return date_prefix
        # e.g splited_file_name = ["Remuneracao", "YYYY", "MM.csv"]
        date_prefix = f"{splited_file_name[1]}{splited_file_name[2][:-4]}"
        return date_prefix

    def upload_files_to_gcs(self, bucket, file_names_to_send, file_blob_map):
        """
        Uploading files into Google Cloud Storage.
        """
        gcs_handle = GcsHandle(bucket, self.logger)
        gcs_handle.upload_blobs_to_gcs(
            self.local_dir, file_names_to_send, file_blob_map
        )

    def append_file_blob_map(self, new_file_name, file_blob_map, structure_dir):
        """
        Appending dictionary with target blobs.
        """
        if new_file_name.endswith("Remuneracao.csv"):
            file_blob_map[new_file_name] = (
                f"SERVIDORES/ESTADUAIS/STAGING/DF/REMUNERACAO/REMUNERACAO/{structure_dir}"
            )
        if new_file_name.endswith("Remuneracao_Detalhamento.csv"):
            file_blob_map[new_file_name] = (
                f"SERVIDORES/ESTADUAIS/STAGING/DF/REMUNERACAO/REMUNERACAODETALHAMENTO/{structure_dir}"
            )

    def get_new_file_name(self, file):
        """
        Returns the new file name.
        """
        # Standardizing file names e.g "YYYY_Remuneracao.csv" or "YYYY_Remuneracao_Detalhamento.csv"
        splited_file = file.split("_")
        date_prefix = self.get_date_prefix(file, splited_file)
        new_file_name = f"{date_prefix}_{file[:-12]}.csv"
        return new_file_name

    def read_csv_file(self, file_path):
        """
        Read file on Pandas DataFrame.
        """
        dataframe = self.pd_csv_handle.read_csv_file(
            file_path,
            sep=";",
            encoding="ISO-8859-1",
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

    def deleting_files_in_gcs(self, bucket, file_name, list_blobs, tag_name):
        """
        Deleting files stored at earlier dates.
        """
        gcs_handle = GcsHandle(bucket, self.logger)
        for blob in list_blobs:
            if (
                blob.exists()
                and file_name in blob.name
                and self.target_date == tag_name
            ):
                gcs_handle.delete_blob(blob)

    @staticmethod
    def rename_columns_df(dataframe):
        """
        Renames columns in the given dataframe.
        """
        columns = {"MÊS": "MES_REFERENCIA", "ANO": "ANO_REFERENCIA"}
        dataframe.rename(columns=columns, inplace=True)

    def run(self):
        """
        Runs the process.
        """
        year, month, day = self.dates_for_blobs
        bucket = self.instantiate_bucket()
        url = self.get_url(self.target_date)
        zipfile_path = self.get_download_path(self.target_date)
        file_names = self.get_file_names()
        list_blobs = self.get_blobs_to_validate(bucket)
        file_exists_in_bucket = self.checking_if_file_exists_in_gcs(
            list_blobs, file_names, year
        )
        if not file_exists_in_bucket:
            self.logger.info(
                f"Starting the download for the Public Servants (DF) - Remuneração from {self.target_date}."
            )
            context = self.set_ssl_context()
            self.download_file(url, context, zipfile_path)
            # Change the directory of "dataproc job" to "/tmp"
            os.chdir("../")
            file_exists = self.validate_if_file_exists(zipfile_path)
            zipfile_is_valid = self.validate_if_zipfile_is_valid(zipfile_path)
            if zipfile_is_valid and file_exists:
                self.unzip_files(zipfile_path)
                file_blob_map = {}
                list_new_file_names = []
                structure_dir = f"ANO={year}/MES={month}/DIA={day}/"
                subdir_path = os.path.join(
                    self.local_dir, f"Remuneracao_{self.target_date}"
                )
                if os.path.exists(subdir_path):
                    for file in os.listdir(subdir_path):
                        new_file_name = self.get_new_file_name(file)
                        old_file_path = os.path.join(subdir_path, file)
                        new_file_path = os.path.join(subdir_path, new_file_name)
                        os.renames(old_file_path, new_file_path)
                        dest_path = os.path.join(self.local_dir, new_file_name)
                        dataframe = self.read_csv_file(new_file_path)
                        self.rename_columns_df(dataframe)
                        self.dataframe_to_csv(dataframe, dest_path)
                        list_new_file_names.append(new_file_name)
                        self.append_file_blob_map(
                            new_file_name, file_blob_map, structure_dir
                        )
                        self.deleting_files_in_gcs(
                            bucket, new_file_name, list_blobs, year
                        )
                    self.upload_files_to_gcs(bucket, list_new_file_names, file_blob_map)


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

    files = [
        "Remuneracao.csv",
        "Remuneracao_Detalhamento.csv",
    ]

    logger.info(f"{'*' * 50} TASK STARTED {'*' * 50}")

    # Setting parameters for the date
    periods_to_check = 11
    current_date = CurrentDate()
    today = current_date.get_today()
    year, month, day = (today.strftime(fmt) for fmt in ["%Y", "%m", "%d"])
    dates_for_blobs = [year, month, day]

    year_date_handle = YearlyDateHandle(today)

    # Execute the tasks in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for period in range(periods_to_check):
            target_date = year_date_handle.get_target_year_date(period, "%Y")
            process_runner = ProcessRunner(
                PROJECT_ID,
                BUCKET_NAME,
                TMP_DIR,
                files,
                target_date,
                dates_for_blobs,
                logger,
            )
            futures.append(executor.submit(process_runner.run))

    logger.info(f"{'*' * 50} TASK FINISHED {'*' * 50}")
    stopwatch.stop()
    stopwatch.elapsed_time(logger)


if __name__ == "__main__":
    main()
