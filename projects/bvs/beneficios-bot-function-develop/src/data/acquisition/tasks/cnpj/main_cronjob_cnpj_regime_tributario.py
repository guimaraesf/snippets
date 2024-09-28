#!/usr/bin/env python
# -*- coding: utf-8 -*-


# ================================================================================================
# Module: main_cronjob_cnpj.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data related to Cadastro Nacional da Pessoa Jurídica - CNPJ
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


def send_files_to_gcp(bucket, path):
    """
        Function to send the downloaded files to a gcp bucket.
    Args:
        bucket: A GCP bucket object where the files will be uploaded.
        path: A string representing the path of the directory where the downloaded files are located.

    Returns:
        None
    """
    file_blob_map = {
        "ImunesIsentas.csv": "CNPJ/REGIMETRIBUTARIO/ISENTAS/ENTRADA",
        "LucroArbitrado.csv": "CNPJ/REGIMETRIBUTARIO/LUCROARBITRADO/ENTRADA",
        "LucroReal.csv": "CNPJ/REGIMETRIBUTARIO/LUCROREAL/ENTRADA",
        "LucroPresumido.csv": "CNPJ/REGIMETRIBUTARIO/LUCROPRESUMIDO/ENTRADA",
    }

    for file in os.listdir(path):
        if file in file_blob_map:
            blob_path = file_blob_map[file]
            blob = bucket.blob(os.path.join(blob_path, file))
            write_file(blob, os.path.join(path, file))


def download_url(url: str, file: str, save_path: str) -> None:
    """
    Downloads a file from a given URL and saves it to a local directory.

    Args:
        url: A string representing the URL of the file to be downloaded.
        file: A string representing the of the file name to be downloaded.
        save_path: A string representing the local path where the file will be saved.

    Returns:
        None
    """
    try:
        context = ssl.create_default_context()
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED

        # Open the URL and read its contents
        html = urlopen(url).read()

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Find all elements with the class "td"
        links = soup.find_all("a", href=True)

        logging.info("Extracting the href attribute.")
        # Loop through each "td" element and extract its "href" attribute
        for link in links:
            href = link["href"]
            download_url = urljoin(url, href)
            file_name = download_url.split("/")[-1]
            if file in file_name and download_url.endswith(".zip"):
                logging.info(f"Downloading file to: {save_path}")
                with urllib.request.urlopen(
                    download_url, context=context
                ) as response, open(save_path, "wb") as outfile:
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


def read_and_concatenate(path: str) -> pd.DataFrame:
    """
    Reads all CSV files from a given directory path into a list of DataFrames,
    concatenates them into a single DataFrame and returns it.

    Args:
        path (str): The path to the directory containing the CSV files to be read.

    Returns:
        df (pd.DataFrame): The concatenated DataFrame.
    """

    list_of_df = []
    logging.info("Reading CSV files into DataFrame.")
    for file in os.listdir(path):
        if file.startswith("Lucro Presumido") and file.endswith(".csv"):
            file_name_path = os.path.join(path, file)
            df = pd.read_csv(
                file_name_path, delimiter=",", encoding="ISO-8859-1", low_memory=False
            )
            list_of_df.append(df)

    logging.info("Concatenating Dataframes.")
    df = pd.concat(list_of_df)

    return df


def df_to_csv(path: str, df: pd.DataFrame):
    """
    Writes a given DataFrame to a CSV file at a given path.

    Args:
        path (str): The path to write the CSV file to.
        df (pd.DataFrame): The DataFrame to be written to the CSV file.

    Returns:
    """
    df.to_csv(path, sep=";", encoding="ISO-8859-1", index=False)


def process_dataframe(path: str, file_name: str):
    """
    Reads all CSV files using the read_and_concatenate function, concatenates them into a single DataFrame
    and writes it to a CSV file at a given path using the df_to_csv function.

    Args:
        path (str): The path to the directory containing the CSV files to be read.
        file_name (str):The name of the CSV file to write the concatenated DataFrame to.

    Returns:
    """
    file_name_path = os.path.join(path, file_name)
    df = read_and_concatenate(path)
    df = df_to_csv(file_name_path, df)

    logging.info(f"Files successfully saved as csv to: {file_name_path}")


def extract_zip_name():
    """Extract the names of zip files from a URL.

    Args:
        None

    Returns:
        list: A list of zip file names.
    """
    # Open the URL and read its contents
    url = "https://dadosabertos.rfb.gov.br/CNPJ/regime_tributario/"
    zipfiles_name = []

    html = urlopen(url).read()

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Find all elements with the class "td"
    links = soup.find_all("a", href=True)

    logging.info("Extracting the href attribute.")
    # Loop through each "td" element and extract its "href" attribute
    for link in links:
        href = link["href"]
        if href.endswith(".zip"):
            file_name = href.split(".")[0]
            zipfiles_name.append(file_name)

    return zipfiles_name


def main():
    logging.basicConfig(level=logging.INFO)
    bucket_dir = sys.argv[2]  # i.e.: hml-gcp-dados-alternativos
    project_id = sys.argv[3]  # i.e.: data-88d7
    local_dir = sys.argv[4]
    bucket = configure_storage(project_id, bucket_dir)

    zipfiles_name = extract_zip_name()
    files_name = {
        zipfile: (
            ["Imunes e isentas.csv", "ImunesIsentas.csv"]
            if zipfile.endswith("isentas")
            else (
                ["Lucro Arbitrado.csv", "LucroArbitrado.csv"]
                if zipfile.endswith("Arbitrado")
                else (
                    ["Lucro Real.csv", "LucroReal.csv"]
                    if zipfile.endswith("Real")
                    else (
                        ["Lucro Presumido.csv", "LucroPresumido.csv"]
                        if zipfile.endswith("Presumido")
                        else None
                    )
                )
            )
        )
        for zipfile in zipfiles_name
    }

    for file in files_name.keys():
        url = f"https://dadosabertos.rfb.gov.br/CNPJ/regime_tributario/"
        save_path = os.path.join(local_dir, f"{file}.zip")
        is_file_available = False

        logging.info(
            f"Starting the download for the National Register of Legal Entities."
        )
        download_url(url, file, save_path)
        is_file_available = True
        try:
            zipfile_name = f"{file}.zip"
            zipfile_path = os.path.join(local_dir, zipfile_name)

            if os.path.exists(zipfile_path) and zipfile.is_zipfile(zipfile_path):
                # Change the directory of "dataproc job" to "/tmp"
                os.chdir("../")
                unzip_files(local_dir, zipfile_path)

                if "Lucro%20Presumido" not in file:
                    old_file_path = os.path.join(local_dir, files_name[file][0])
                    new_file_path = os.path.join(local_dir, files_name[file][1])
                    os.rename(old_file_path, new_file_path)
                    logging.info(f"Renamed {old_file_path} to {new_file_path}")

                    logging.info(f"Reading CSV files into DataFrame.")
                    df = pd.read_csv(
                        new_file_path,
                        delimiter=",",
                        encoding="ISO-8859-1",
                        low_memory=False,
                    )
                    df.to_csv(
                        new_file_path, sep=";", encoding="ISO-8859-1", index=False
                    )

                    logging.info(f"Files successfully saved as csv to: {new_file_path}")

            else:
                logging.error(f"Zipfile {zipfile_name} not found or invalid")

        except FileNotFoundError as e:
            logging.error(f"Zip file {zipfile_name} not found: {e}")

        except zipfile.BadZipFile as e:
            logging.error(f"Failed to extract zip file {zipfile_name}: {e}")

    # Processes and concatenates files on the "Lucro Presumido"
    process_dataframe(local_dir, "LucroPresumido.csv")

    # Upload the unzipped file to the bucket
    if is_file_available:
        send_files_to_gcp(bucket, local_dir)

    logging.info("Cronjob successfully finished")


if __name__ == "__main__":
    main()
