#!/usr/bin/env python
# -*- coding: utf-8 -*-


# ================================================================================================
# Module: main_cronjob_ibama.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data related to Ibama Self-Infringement
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
import logging
import zipfile
import ssl
import urllib
import urllib.request
import urllib.error
import pandas as pd
from typing import List
from google.api_core.exceptions import NotFound
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
    file_blob_map = {f"{date}_AutosInfracao.csv": "IBAMA/ENTRADA"}

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
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
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


def delete_blob(bucket, blob_name: str, file_name: str):
    """Deletes a blob from the bucket.
    Args:
        bucket (storage.bucket.Bucket): The bucket containing the blob to be deleted.
        blob_name (str): The name of the blob to be deleted.
        file_name (str): The name of the file to be deleted.
    """
    blob = bucket.blob(os.path.join(blob_name, file_name))
    blob.delete()

    logging.info(f"Blob has been deleted: {blob}")


def fix_columns(df, string_to_replace: str, replacement: str):
    """
    Replaces occurrences of a string in specific columns of a DataFrame.

    Args:
        df (pandas.Dataframe): Input DataFrame.
        string_to_replace (str): String to be replaced.
        replacement (str): Replacement string.

    Returns:
        Pandas DataFrame with replacements performed.
    """
    for column in df.columns:
        if df[column].astype(str).str.contains(string_to_replace).any():
            df[column] = df[column].str.replace(string_to_replace, replacement)

    return df


def main():
    logging.basicConfig(level=logging.INFO)
    bucket_dir = sys.argv[2]  # i.e.: hml-gcp-dados-alternativos
    project_id = sys.argv[3]  # i.e.: data-88d7
    local_dir = sys.argv[4]
    bucket = configure_storage(project_id, bucket_dir)

    days_to_check = 1
    today = date.today()

    for i in range(0, days_to_check):
        # Setting parameters for scaning the previous days
        target_day = today - relativedelta(days=i)
        dt_refer = target_day.strftime("%Y%m%d")
        dt_name = target_day.strftime("%d %B %Y")

        # Delete files from the previous day
        blob_name = "IBAMA/ENTRADA"
        blobs_ud = bucket.list_blobs()

        previous_target_day = target_day - relativedelta(days=1)
        previous_date = previous_target_day.strftime("%Y%m%d")
        previous_file_name = f"{previous_date}_AutosInfracao.csv"

        url = "https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao.csv"
        save_path = os.path.join(local_dir, "auto_infracao.csv")
        file_name = f"{dt_refer}_AutosInfracao.csv"
        is_file_available = False

        logging.info(f"Starting the download for the Ibama Self-Infringement.")
        # Check that the downloaded file exists
        if validate_existing_file(bucket, file_name):
            # Download the file
            download_url(url, save_path)
            is_file_available = True

            try:
                if os.path.exists(save_path):
                    # Change the directory of "dataproc job" to "/tmp"
                    os.chdir("../")
                    old_file_path = os.path.join(local_dir, "auto_infracao.csv")
                    new_file_path = os.path.join(
                        local_dir, f"{dt_refer}_AutosInfracao.csv"
                    )

                    os.rename(old_file_path, new_file_path)
                    logging.info(f"Renamed {old_file_path} to {new_file_path}")

                    logging.info("Reading CSV files into DataFrame.")
                    df = pd.read_csv(
                        new_file_path, sep=";", encoding="UTF-8", low_memory=False
                    )

                    logging.info("Applying fixes on the DataFrame.")
                    str_to_replace = ";"
                    replacement_str = " "

                    df = fix_columns(df, str_to_replace, replacement_str)
                    df.to_csv(
                        new_file_path, sep=";", encoding="ISO-8859-1", index=False
                    )

                else:
                    logging.error(f"CSV file {save_path} not found or invalid")

            except FileNotFoundError as e:
                logging.error(f"CSV file {save_path} not found: {e}")

        # Upload the unzipped file to the bucket
        if is_file_available:
            send_files_to_gcp(bucket, local_dir, dt_refer)

            try:
                for blob in blobs_ud:
                    blob_path = os.path.join(blob_name, previous_file_name)
                    if blob_path in blob.name:
                        delete_blob(bucket, blob_name, previous_file_name)

            except NotFound as e:
                logging.error(f"Blob not found: {e}")

    logging.info("Cronjob successfully finished")


if __name__ == "__main__":
    main()
