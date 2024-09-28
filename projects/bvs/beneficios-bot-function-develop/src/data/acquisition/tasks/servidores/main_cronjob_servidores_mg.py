#!/usr/bin/env python
# -*- coding: utf-8 -*-


# ================================================================================================
# Module: main_cronjob_servidores_mg.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data on remuneration of Public Servants (MG)
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
import logging
import calendar
import ssl
import urllib
import urllib.request
import urllib.error
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
        file_name = f"{date}_Servidores_MG.csv"
        if file_name in file:
            blob_name = f"SERVIDORES/ESTADUAIS/MG/ENTRADA/{file}"
            blob = bucket.blob(blob_name)
            write_file(blob, os.path.join(path, file))


def check_size_and_delete_file(file_path):
    """
        Checks if the file is empty, if so, it is deleted
    Args:
        file_path: A string representing the path of the directory where the downloaded files are located.

    Returns:
        Delete empty file.
    """
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.warning("File is empty, deleting downloaded file.")


def download_url(url, save_path):
    """
    Args:
        url: The URL of the file to be downloaded.
        save_path: The path where the downloaded file will be saved.

    Returns:
        None
    """

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

    logging.info(f"Downloading file to: {save_path}")
    try:
        context = ssl.create_default_context()
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response, open(save_path, "wb") as outfile:
            outfile.write(response.read())
        logging.info("File was downloaded successfully.")

        # Checking if the file is empty, if yes, the file is deleted.
        check_size_and_delete_file(save_path)

    except urllib.error.HTTPError as e:
        logging.error(f"Unable to download file: HTTP Error {e.code}: {e.reason}")


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
        dt_url = last_day_of_month.strftime("%m%-y")

        url = f"https://www.transparencia.mg.gov.br/estado-pessoal/index.php?option=com_transparenciamg&task=estado_remuneracao.downloadRemuneracao&periodo={dt_url}"
        save_path = os.path.join(local_dir, f"ServidoresMG_{dt_url}.csv")
        file_name = f"{dt_refer}_Servidores_MG.csv"
        is_file_available = False

        logging.info(
            f"Starting the download for the Minas Gerais Public Servants from {dt_name}."
        )
        # Check that the downloaded file exists
        if validate_existing_file(bucket, file_name):
            download_url(url, save_path)
            is_file_available = True

            try:
                if os.path.exists(save_path):
                    # Change the directory of "dataproc job" to "/tmp"
                    os.chdir("../")
                    old_file_path = os.path.join(
                        local_dir, f"ServidoresMG_{dt_url}.csv"
                    )
                    new_file_path = os.path.join(
                        local_dir, f"{dt_refer}_Servidores_MG.csv"
                    )

                    os.rename(old_file_path, new_file_path)
                    logging.info(f"Renamed {old_file_path} to {new_file_path}")

            except FileNotFoundError as e:
                logging.error(f"File not found: {e}")

        # Upload the unzipped file to the bucket
        if is_file_available:
            send_files_to_gcp(bucket, local_dir, dt_refer)

    logging.info("Cronjob successfully finished")


if __name__ == "__main__":
    main()
