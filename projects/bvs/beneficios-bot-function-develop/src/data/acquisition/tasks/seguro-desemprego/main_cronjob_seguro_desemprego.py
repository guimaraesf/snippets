#!/usr/bin/env python
# -*- coding: utf-8 -*-


# ================================================================================================
# Module: main_cronjob_seguro_desemprego.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data related to Seguro Desemprego payments
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


def send_files_to_gcp(bucket, path, date, modality):
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
        file_name = f"SD_Beneficios_{date}_{str(modality)}.csv"
        if file_name not in file:
            continue
        modality_names = [
            "TrabalhadorFormal",
            "EmpregadoDomestico",
            "TrabalhadorResgatado",
            "BolsaQualificacao",
            "PescadorArtesanal",
        ]
        modality_prefix = [
            "TRABFORMAL",
            "EMPRDOMESTICO",
            "TRABRESGATADO",
            "BOLSAQUALIFIC",
            "PESCARTESANAL",
        ]

        if modality not in range(len(modality_names)):
            logging.warning(f"Invalid modality {modality}. Skipping file {file_name}.")
            continue

        old_file_path = os.path.join(path, file)
        new_file_name = os.path.join(path, f"{date}_{modality_names[modality]}.csv")
        os.rename(old_file_path, new_file_name)

        blob_name = f"SEGURODESEMPREGO/{modality_prefix[modality]}/ENTRADA/{date}_{modality_names[modality]}.csv"
        blob = bucket.blob(blob_name)
        write_file(blob, new_file_name)


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
        # The website did not have an SSL Certificate
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(url, context=context) as response, open(
            save_path, "wb"
        ) as outfile:
            outfile.write(response.read())
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


def get_file_name(date, file):
    file_names = {
        0: "TrabalhadorFormal",
        1: "EmpregadoDomestico",
        2: "TrabalhadorResgatado",
        3: "BolsaQualificacao",
        4: "PescadorArtesanal",
    }

    if file in file_names:
        return f"{date}_{file_names[file]}.csv"

    else:
        logging.info(f"Invalid file code: {file}")
        return None


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

        for modality in range(5):
            url = f"https://transparencia.sd.mte.gov.br/arquivos/SD_Beneficios_{dt_refer}_{str(modality)}.zip"
            save_path = os.path.join(
                local_dir, f"seguro_desemprego{dt_refer}_{str(modality)}.zip"
            )
            file_name = get_file_name(dt_refer, modality)

            is_file_available = False

            logging.info(
                f"Starting the download for the {file_name[7:-4]} social benefit from {dt_name}."
            )
            # Check that the downloaded file exists
            if validate_existing_file(bucket, file_name):
                # Download the file
                download_url(url, save_path)
                is_file_available = True

                # Unzip the file
                try:
                    zipfile_name = f"seguro_desemprego{dt_refer}_{str(modality)}.zip"
                    zipfile_path = os.path.join(local_dir, zipfile_name)

                    if os.path.exists(zipfile_path) and zipfile.is_zipfile(
                        zipfile_path
                    ):
                        os.chdir("../")
                        unzip_files(local_dir, zipfile_path)
                    else:
                        logging.error(f"Zipfile {zipfile_name} not found or invalid")

                except FileNotFoundError as e:
                    logging.error(f"Zip file {zipfile_name} not found: {e}")

                except zipfile.BadZipFile as e:
                    logging.error(f"Failed to extract zip file {zipfile_name}: {e}")

            # Upload the unzipped file to the bucket
            if is_file_available:
                send_files_to_gcp(bucket, local_dir, dt_refer, modality)

    logging.info("Cronjob successfully finished")


if __name__ == "__main__":
    main()
