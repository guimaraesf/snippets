#!/usr/bin/env python
# -*- coding: utf-8 -*-


# ================================================================================================
# Module: main_cronjob_servidores_sc.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data on remuneration of Public Servants (SC)
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
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.request import urlopen
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


def send_files_to_gcp(bucket, path, date, file_name):
    """
        Function to send the downloaded files to a gcp bucket.
    Args:
        bucket: A GCP bucket object where the files will be uploaded.
        path: A string representing the path of the directory where the downloaded files are located.

    Returns:
        None
    """
    file_blob_map = {f"{date}_Servidores_SC.csv": "SERVIDORES/ESTADUAIS/SC/ENTRADA"}

    if os.path.exists(os.path.join(path, file_name)):
        blob_path = file_blob_map[file_name]
        blob = bucket.blob(os.path.join(blob_path, file_name))
        write_file(blob, os.path.join(path, file_name))


def download_url(url, date, saved_path) -> None:
    """
    Downloads a file from a given URL and saves it to a local directory.

    Args:
        url: A string representing the URL of the file to be downloaded.
        file: A string representing the of the file name to be downloaded.
        saved_path: A string representing the local saved path where the file will be saved.

    Returns:
        None
    """
    try:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        # Open the URL and read its contents
        html = urlopen(url=url, context=context).read()

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Find all elements with the class "td"
        links = soup.find_all("a", href=True)

        # Loop through each "td" element and extract its "href" attribute
        for link in links:
            href = link["href"]
            files_name = [
                f"servidores-ativos-{date}.csv",
                f"servidores-inativos-{date}.csv",
            ]
            if not any(href.endswith(file) for file in files_name):
                continue

            file_name = href.split("/")[-1]
            file_path = os.path.join(saved_path, file_name)
            logging.info(f"Downloading file to: {file_path}")
            with urllib.request.urlopen(href, context=context) as response, open(
                file_path, "wb"
            ) as outfile:
                outfile.write(response.read())
                logging.info(f"File was downloaded successfully.")

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
        file_name: A list of strings representing the name of the file to check for.

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


def get_first_day_of_month():
    """
    Get the first day of the current month.

    Returns:
        datetime.date: The first day of the current month.
    """
    today = date.today()
    first_day = today.replace(day=1)

    return first_day


def get_last_day_of_month(first_day_of_month, number_of_months):
    """
    Get the last day of the specified month.

    Args:
        first_day_of_month (datetime.date): The first day of the reference month.
        number_of_months (int): The number of months to go back from the reference month.

    Returns:
        datetime.date: The last day of the specified month.
    """
    target_month = first_day_of_month - relativedelta(months=number_of_months)
    days_in_month = calendar.monthrange(target_month.year, target_month.month)[1]
    last_day_of_month = target_month.replace(day=days_in_month)

    return last_day_of_month


def read_file(file_path):
    """
    Reads a CSV file from the given file path and returns a DataFrame.

    Args:
        file_path (str): The path of the CSV file to read.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the CSV file.
    """
    df = pd.read_csv(file_path, sep=";", encoding="ISO-8859-1", low_memory=False)
    return df


def convert_to_csv_file(df, file_path):
    """
    Converts a DataFrame to a CSV file and saves it to the given file path.

    Args:
        df (pd.DataFrame) : The DataFrame to convert to a CSV file.
        file_path (str): The path where the CSV file will be saved.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the CSV file.
    """
    df.to_csv(file_path, sep=";", encoding="ISO-8859-1", index=False)


def concat_dfs(dfs):
    """
    Concatenates a list of DataFrames into a single DataFrame.

    Args:
        dfs: A list of DataFrames to concatenate.

    Returns:
        pd.DataFrame: A concatenated DataFrame.
    """
    df = pd.concat(dfs)
    return df


def process_df(file_path_table_1, file_path_table_2, target_date, destination_path):
    """
    Processes two CSV files by reading them into DataFrames, concatenating them,
    adding new columns with year and month information, and saving the resulting
    DataFrame as a new CSV file.

    Args:
        file_path_table_1: The path of the first CSV file to read.
        file_path_table_2: The path of the second CSV file to read.
        target_date: The date used to generate the year and month information for the new columns. It should be in the format "YYYY-MM-DD".
        destination_path: The path where the resulting CSV file will be saved. It should include the name of the resulting CSV file.

    Returns:
        None
    """
    logging.info("Reading DataFrame.")
    df_ativos = read_file(file_path_table_1)
    df_inativos = read_file(file_path_table_2)

    logging.info("Concatenating Ativos and Inativos DataFrames.")
    df_concatenated = concat_dfs([df_ativos, df_inativos])
    df_concatenated["Ano"] = target_date[:4]
    df_concatenated["Mes"] = target_date[6:]
    convert_to_csv_file(df_concatenated, destination_path)

    logging.info(f"Files successfully saved as csv to: {destination_path}")


def main():
    logging.basicConfig(level=logging.INFO)
    bucket_dir = sys.argv[2]  # i.e.: hml-gcp-dados-alternativos
    project_id = sys.argv[3]  # i.e.: data-88d7
    local_dir = sys.argv[4]
    bucket = configure_storage(project_id, bucket_dir)

    months_to_check = 6
    first_day_of_month = get_first_day_of_month()

    for i in range(1, months_to_check + 1):
        # Setting parameters for scaning the previous months
        last_day_of_month = get_last_day_of_month(first_day_of_month, i)
        dt_refer = last_day_of_month.strftime("%Y-%m")
        dt_new_refer = last_day_of_month.strftime("%Y%m")
        dt_name = last_day_of_month.strftime("%B %Y")

        url = "https://dados.sc.gov.br/dataset/remuneracaoservidores"
        file_name = f"{dt_new_refer}_Servidores_SC.csv"
        is_file_available = False

        logging.info(
            f"Starting the download for the Public Servants (SC) from {dt_name}."
        )
        if validate_existing_file(bucket, file_name):
            download_url(url, dt_refer, local_dir)
            is_file_available = True
            try:
                file_name_ativos = f"servidores-ativos-{dt_refer}.csv"
                file_path_ativos = os.path.join(local_dir, file_name_ativos)
                file_name_inativos = f"servidores-inativos-{dt_refer}.csv"
                file_path_inativos = os.path.join(local_dir, file_name_inativos)
                destination_path = os.path.join(local_dir, file_name)
                if any(
                    os.path.exists(file_path)
                    for file_path in [file_path_ativos, file_path_inativos]
                ):
                    # Change the directory of "dataproc job" to "/tmp"
                    os.chdir("../")
                    logging.info("Running DataFrame processing.")
                    process_df(
                        file_path_ativos, file_path_inativos, dt_refer, destination_path
                    )

            except FileNotFoundError as e:
                logging.error(f"Zip file {zipfile_name} not found: {e}")

            except zipfile.BadZipFile as e:
                logging.error(f"Failed to extract zip file {zipfile_name}: {e}")

        # Upload the unzipped file to the bucket
        if is_file_available:
            send_files_to_gcp(bucket, local_dir, dt_new_refer, file_name)

    logging.info("Cronjob successfully finished")


if __name__ == "__main__":
    main()
