#!/usr/bin/env python
# -*- coding: utf-8 -*-


# ================================================================================================
# Module: main_cronjob_fornecedores_gov.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data related to government suppliers
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
import logging
import zipfile
import calendar
import ssl
import urllib
import urllib.request
import urllib.error
import pandas as pd
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
    file_blob_map = {
        f"{date}_Compras.csv": "FORNECEDORES/COMPRAS/ENTRADA",
        f"{date}_ItemCompra.csv": "FORNECEDORES/ITEMCOMPRA/ENTRADA",
        f"{date}_TermoAditivo.csv": "FORNECEDORES/TERMOADITIVO/ENTRADA",
    }

    for file in os.listdir(path):
        if file in file_blob_map:
            blob_path = file_blob_map[file]
            blob = bucket.blob(os.path.join(blob_path, file))
            write_file(blob, os.path.join(path, file))


def download_url(url: str, save_path: str) -> None:
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
        context = ssl.create_default_context()
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        with urllib.request.urlopen(url, context=context) as response:
            with open(save_path, "wb") as f:
                f.write(response.read())

        logging.info("File was downloaded successfully.")

    except urllib.error.HTTPError as e:
        logging.error(f"Unable to download file: HTTP Error {e.code}: {e.reason}")


def unzip_files(extract_path: str, file_path: str) -> None:
    """
    Extracts a zip file to the specified directory.

    Args:
        file_path: A string representing the local path of the zip file to extract.
        extract_path: A string representing the local directory where the files should be extracted.

    Returns:
        None
    """

    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)
        logging.info(f"All files extracted from {file_path} to {extract_path}.")


def validate_existing_file(bucket, files_name):
    """
    Checks if a file with the same name already exists in a GCP bucket.

    Args:
        bucket: A Bucket object representing the GCP bucket to check.
        files_name: A list of strings representing the name of the file to check for.

    Returns:
        A boolean value indicating if a file with the same name already exists in the bucket.
    """
    logging.info("Checking if the file already exists in the bucket")

    file_blob_map = {
        files_name[0]: f"FORNECEDORES/COMPRAS/ENTRADA/{files_name[0]}",
        files_name[1]: f"FORNECEDORES/ITEMCOMPRA/ENTRADA/{files_name[1]}",
        files_name[2]: f"FORNECEDORES/TERMOADITIVO/ENTRADA/{files_name[2]}",
    }

    file_exists = True
    for file_name in files_name:
        blob = bucket.get_blob(file_blob_map[file_name])
        if blob:
            logging.info(f"File {file_name} already exists in the bucket.")
            file_exists = False

        else:
            logging.info(f"File {file_name} does not exist in the bucket.")

    return file_exists


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

        url = f"https://www.portaldatransparencia.gov.br/download-de-dados/compras/{dt_refer}"
        save_path = os.path.join(local_dir, f"compras_{dt_refer}.zip")
        files_name = [
            f"{dt_refer}_Compras.csv",
            f"{dt_refer}_ItemCompra.csv",
            f"{dt_refer}_TermoAditivo.csv",
        ]

        is_file_available = False

        logging.info(
            f"Starting the download for the Government Suppliers from {dt_name}."
        )
        # Check that the downloaded file exists
        if validate_existing_file(bucket, files_name):
            # Download the file
            download_url(url, save_path)
            is_file_available = True

            # Unzip the file
            try:
                zipfile_name = f"compras_{dt_refer}.zip"
                zipfile_path = os.path.join(local_dir, zipfile_name)

                if os.path.exists(zipfile_path) and zipfile.is_zipfile(zipfile_path):
                    # Change the directory of "dataproc job" to "/tmp"
                    os.chdir("../")
                    unzip_files(local_dir, zipfile_path)

                    compras_file = os.path.join(local_dir, f"{dt_refer}_Compras.csv")
                    item_compra_file = os.path.join(
                        local_dir, f"{dt_refer}_ItemCompra.csv"
                    )

                    logging.info("Reading CSV files into DataFrames.")
                    df_compras = pd.read_csv(
                        compras_file, sep=";", encoding="ISO-8859-1", low_memory=False
                    )
                    df_item_compra = pd.read_csv(
                        item_compra_file,
                        sep=";",
                        encoding="ISO-8859-1",
                        low_memory=False,
                    )

                    df_compras.to_csv(
                        compras_file, sep=";", encoding="ISO-8859-1", index=False
                    )
                    df_item_compra.to_csv(
                        item_compra_file, sep=";", encoding="ISO-8859-1", index=False
                    )

                    logging.info(
                        f"Files successfully saved as csv to: {compras_file} and {item_compra_file}"
                    )

                else:
                    logging.error(f"Zipfile {zipfile_name} not found or invalid")

            except FileNotFoundError as e:
                logging.error(f"Zip file {zipfile_name} not found: {e}")

            except zipfile.BadZipFile as e:
                logging.error(f"Failed to extract zip file {zipfile_name}: {e}")

        # Upload the unzipped file to the bucket
        if is_file_available:
            send_files_to_gcp(bucket, local_dir, dt_refer)

    logging.info("Cronjob successfully finished")


if __name__ == "__main__":
    main()
