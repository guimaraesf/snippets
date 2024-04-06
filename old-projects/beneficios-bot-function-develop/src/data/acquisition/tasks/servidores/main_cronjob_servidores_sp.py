#!/usr/bin/env python
# -*- coding: utf-8 -*-


# ================================================================================================
# Module: main_cronjob_servidores_sp.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data on remuneration of Public Servants (SP)
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
import logging
import calendar
import urllib
import urllib.request
import urllib.error
import rarfile
import requests
import pandas as pd
from rarfile import RarFile
from google.cloud import storage
from datetime import date
from dateutil.relativedelta import relativedelta


def configure_storage(project_id: str, bucket_name: str) -> storage.Bucket:
    """
    Configures the GCP client and returns a bucket object.

    Args:
        project_id: A string representing the project ID where the bucket is located.
        bucket_name: A string representing the name of the bucket to access.

    Returns:
        A Bucket object representing the specified GCP bucket.
    """

    logging.info(f"Configuring the application to access the GCP bucket: {bucket_name}")
    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(bucket_name)

    return bucket


def write_file(blob, path_file):
    """
    Uploads a file to a GCP bucket.

    Args:
        blob: A Blob object representing the GCP bucket and destination file path.
        file_path: A string representing the local file path of the file to be uploaded.

    Returns:
        None

    """
    try:
        logging.info(f"Uploading file to GCP: {blob}")
        with open(path_file, "rb") as zip_file:
            blob.upload_from_file(zip_file, content_type="application/zip")
            logging.info("File uploaded successfully.")

    except Exception as e:
        logging.error(f"Error uploading file to GCP: {e}")


def send_files_to_gcp(bucket, path, date):
    """
        Function to send the downloaded files to a gcp bucket.
    Args:
        bucket: A GCP bucket object where the files will be uploaded.
        path: A string representing the path of the directory where the downloaded files are located.
        date: A string representing the date of the files to be uploaded.

    Returns:
        None
    """
    for file in os.listdir(path):
        file_name = f"{date}_Servidores_SP.csv"
        if file_name in file:
            blob_name = f"SERVIDORES/ESTADUAIS/SP/ENTRADA/{file}"
            blob = bucket.blob(blob_name)
            write_file(blob, os.path.join(path, file))


def download_url(url: str, save_path: str, chunk_size=128) -> None:
    """
    Downloads a file from a given URL and saves it to a local directory.

    Args:
        url: A string representing the URL of the file to be downloaded.
        save_path: A string representing the local path where the file will be saved.

    Returns:
        None
    """
    logging.info(f"Downloading file to: {save_path}")
    try:
        r = requests.get(url, stream=True)

        if r.status_code == 200:
            with open(save_path, "wb") as fd:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)

            logging.info("File was downloaded successfully.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error occurred while downloading {url}: {e}")


def unzip_files(extract_path: str, file_path: str) -> None:
    """
    Extracts a zip file to the specified directory.

    Args:
        file_path: A string representing the local path of the zip file to extract.
        extract_path: A string representing the local directory where the files should be extracted.

    Returns:
        None
    """

    with rarfile.RarFile(file_path, "r") as rf:
        rf.extractall(extract_path)
        logging.info(f"All files extracted from {file_path} to {extract_path}.")


def validate_existing_file(bucket, file_name):
    """
    Checks if a file with the same name already exists in a GCP bucket.

    Args:
        bucket: A Bucket object representing the GCP bucket to check.
        file_name: A string representing the name of the file to check for.

    Returns:
        A boolean value indicating if a file with the same name already exists in the bucket.
    """

    logging.info("Checking if the file already exists in the bucket")
    blobs_ud = bucket.list_blobs()
    for blob in blobs_ud:
        if file_name in blob.name:
            logging.info(f"File {file_name} already exists in the bucket.")
            return False

    logging.info(f"File {file_name} does not exist in the bucket.")
    return True


MONTHS = {
    "January": "Janeiro",
    "February": "Fevereiro",
    "March": "Março",
    "April": "Abril",
    "May": "Maio",
    "June": "Junho",
    "July": "Julho",
    "August": "Agosto",
    "September": "Setembro",
    "October": "Outubro",
    "November": "Novembro",
    "December": "Dezembro",
}


def replace_month_name(month):
    return MONTHS.get(month, month)


def main():
    logging.basicConfig(level=logging.INFO)
    bucket_dir = sys.argv[2]  # i.e.: hml-gcp-dados-alternativos    
    project_id = sys.argv[3]  # i.e.: data-88d7
    local_dir = sys.argv[4]
    bucket = configure_storage(project_id, bucket_dir)

    months_to_check = 6
    today = date.today()
    first_day_of_month = today.replace(day=1)

    for i in range(1, months_to_check + 1):
        # Setting parameters for scaning the previous months
        target_month = first_day_of_month - relativedelta(months=i)
        days_in_month = calendar.monthrange(target_month.year, target_month.month)[1]
        last_day_of_month = target_month.replace(day=days_in_month)
        dt_refer = last_day_of_month.strftime("%Y%m")
        dt_name = last_day_of_month.strftime("%B %Y")
        year = last_day_of_month.strftime("%Y")
        month_en = last_day_of_month.strftime("%B").title()
        month_br = replace_month_name(month_en)

        url = f"https://www.transparencia.sp.gov.br/PortalTransparencia-Report/historico/remuneracao_{month_br}_{year}.rar"
        save_path = os.path.join(local_dir, f"{dt_refer}_servidores_sp.rar")
        file_name = f"{dt_refer}_Servidores_SP.csv"

        is_file_available = False

        logging.info(
            f"Starting the download for the São Paulo Public Servants from {dt_name}."
        )
        # check that the downloaded file exists

        if validate_existing_file(bucket, file_name):
            download_url(url, save_path)
            is_file_available = True

            # Unzip the file
            try:
                rarfile_name = f"{dt_refer}_servidores_sp.rar"
                rarfile_path = os.path.join(local_dir, rarfile_name)

                if os.path.exists(rarfile_path) and rarfile.is_rarfile(rarfile_path):
                    # Change the directory of "dataproc job" to "/tmp"
                    os.chdir("../")
                    unzip_files(local_dir, rarfile_path)

                    # Read .txt file in Dataframe
                    logging.info("Reading TXT file into DataFrame.")
                    for file in os.listdir(local_dir):
                        if file.endswith(".txt") and (
                            "Remuneracao" in file or "remuneracao" in file
                        ):
                            remuneracao_file = os.path.join(local_dir, file)
                            df = pd.read_csv(
                                remuneracao_file, sep=";", encoding="ISO-8859-1"
                            )

                            csv_file_name = os.path.join(
                                local_dir, f"{dt_refer}_Servidores_SP.csv"
                            )
                            df.to_csv(
                                csv_file_name,
                                sep=";",
                                encoding="ISO-8859-1",
                                index=False,
                            )

                    # Upload the unzipped file to the bucket
                    send_files_to_gcp(bucket, local_dir, dt_refer)

            except FileNotFoundError as e:
                logging.error(f"Rar file {rarfile_name} not found: {e}")

            except rarfile.BadRarFile as e:
                logging.error(f"Failed to extract zip file {rarfile_name}: {e}")

    logging.info("Cronjob successfully finished")


if __name__ == "__main__":
    main()
