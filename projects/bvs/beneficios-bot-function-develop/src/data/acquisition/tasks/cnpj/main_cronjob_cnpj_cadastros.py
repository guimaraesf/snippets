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


def send_files_to_gcp(bucket, path, file_name):
    """
        Function to send the downloaded files to a gcp bucket.
    Args:
        bucket: A GCP bucket object where the files will be uploaded.
        path: A string representing the path of the directory where the downloaded files are located.

    Returns:
        None
    """
    file_blob_map = {
        "Cnaes": "CNPJ/CNAES/ENTRADA",
        "Empresas": "CNPJ/EMPRESAS/ENTRADA",
        "Estabelecimentos": "CNPJ/ESTABELECIMENTOS/ENTRADA",
        "Socios": "CNPJ/SOCIOS/ENTRADA",
        "Motivos": "CNPJ/MOTIVOS/ENTRADA",
        "Municipios": "CNPJ/MUNICIPIOS/ENTRADA",
        "Naturezas": "CNPJ/NATUREZASJURIDICAS/ENTRADA",
        "Paises": "CNPJ/PAISES/ENTRADA",
        "Qualificacoes": "CNPJ/QUALIFICACOESSOCIOS/ENTRADA",
        "Simples": "CNPJ/SIMPLES/ENTRADA",
    }

    for file in os.listdir(path):
        if file == file_name:
            for k in file_blob_map.keys():
                if file.startswith(k):
                    blob_path = file_blob_map[k]
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


def extract_file_name(zipfile_path):
    """Extract the first file name from a zip file.

    Args:
        zipfile_path (str): The path of the zip file.
    Returns:
        str: The name of the first file in the zip file.
    """
    with zipfile.ZipFile(zipfile_path, "r") as zip_ref:
        file_name = zip_ref.namelist()

        return file_name[0]


def extract_zip_name():
    """Extract the names of zip files from a URL.

    Args:
        None

    Returns:
        list: A list of zip file names.
    """
    # Open the URL and read its contents
    url = "https://dadosabertos.rfb.gov.br/CNPJ/"
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


class CNPJDataOrganizer:
    """A class to organize lists and dictionaries related to zip files."""

    def __init__(self):
        self.common_columns = ["CODIGO", "DESCRICAO"]
        self.empresas_columns = [
            "CNPJ_BASICO",
            "RAZAO_SOCIAL",
            "NATUREZA_JURIDICA",
            "QUALIFICACAO_DO_RESPONSAVEL",
            "CAPITAL_SOCIAL_DA_EMPRESA",
            "PORTE_DA_EMPRESA",
            "ENTE_FEDERATIVO_RESPONSAVEL",
        ]
        self.estabelecimentos_columns = [
            "CNPJ_BASICO",
            "CNPJ_ORDEM",
            "CNPJ_DV",
            "IDENTIFICADOR",
            "NOME_FANTASIA",
            "SITUACAO_CADASTRAL",
            "DATA_SITUACAO_CADASTRAL",
            "MOTIVO_SITUACAO_CADASTRAL",
            "NOME_DA_CIDADE_NO_EXTERIOR",
            "PAIS",
            "DATA_INICIO_ATIVIDADE",
            "CNAE_FISCAL_PRINCIPAL",
            "CNAE_FISCAL_SECUNDARIA",
            "TIPO_DE_LOGRADOURO",
            "LOGRADOURO",
            "NUMERO",
            "COMPLEMENTO",
            "BAIRRO",
            "CEP",
            "UF",
            "MUNICIPIO",
            "DDD_1",
            "TELEFONE_1",
            "DDD_2",
            "TELEFONE_2",
            "DDD_FAX",
            "FAX",
            "CORREIO_ELETRONICO",
            "SITUACAO_ESPECIAL",
            "DATA_SITUACAO_ESPECIAL",
        ]
        self.socios_columns = [
            "CNPJ_BASICO",
            "IDENTIFICADOR_DE_SOCIO",
            "NOME_DO_SOCIO_OU_RAZAO_SOCIAL",
            "CPF_OU_CNPJ_DO_SOCIO",
            "QUALIFICACAO_DO_SOCIO",
            "DATA_DE_ENTRADA_SOCIEDADE",
            "PAIS",
            "REPRESENTANTE_LEGAL",
            "NOME_DO_REPRESENTANTE_LEGAL",
            "QUALIFICACAO_DO_REPRESENTANTE_LEGAL",
            "FAIXA_ETARIA",
        ]
        self.simples_columns = [
            "CNPJ_BASICO",
            "OPCAO_PELO_SIMPLES",
            "DATA_DE_OPCAO_PELO_SIMPLES",
            "DATA_DE_EXCLUSAO_DO_SIMPLES",
            "OPCAO_PELO_MEI",
            "DATA_DE_OPCAO_PELO_MEI",
            "DATA_DE_EXCLUSAO_DO_MEI",
        ]
        self.zipfiles_name = extract_zip_name()
        self.files_and_columns_name = {
            zipfile: self.empresas_columns
            if zipfile.startswith("Empresas")
            else self.estabelecimentos_columns
            if zipfile.startswith("Estabelecimentos")
            else self.socios_columns
            if zipfile.startswith("Socios")
            else self.simples_columns
            if zipfile.startswith("Simples")
            else self.common_columns
            for zipfile in self.zipfiles_name
        }


def main():
    logging.basicConfig(level=logging.INFO)
    bucket_dir = sys.argv[2]  # i.e.: hml-gcp-dados-alternativos    
    project_id = sys.argv[3]  # i.e.: data-88d7
    local_dir = sys.argv[4]
    bucket = configure_storage(project_id, bucket_dir)

    get_attributes = CNPJDataOrganizer()
    list_of_files = get_attributes.files_and_columns_name

    for file in list_of_files.keys():
        url = "https://dadosabertos.rfb.gov.br/CNPJ/"
        save_path = os.path.join(local_dir, f"{file}.zip")
        is_file_available = False

        logging.info(
            f"Starting the download for the National Register of Legal Entities - {file}."
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

                old_file_name = extract_file_name(zipfile_path)
                new_file_name = f"{file}.csv"
                column_names = get_attributes.files_and_columns_name[file]

                old_file_path = os.path.join(local_dir, old_file_name)
                new_file_path = os.path.join(local_dir, new_file_name)
                os.rename(old_file_path, new_file_path)
                logging.info(f"Renamed {old_file_path} to {new_file_path}")

                logging.info(f"Reading CSV files into DataFrame.")
                df = pd.read_csv(
                    new_file_path,
                    delimiter=";",
                    encoding="ISO-8859-1",
                    header=None,
                    low_memory=False,
                )
                df.columns = column_names
                df.to_csv(new_file_path, sep=";", encoding="ISO-8859-1", index=False)

                logging.info(f"Files successfully saved as csv to: {new_file_path}")

            else:
                logging.error(f"Zipfile {zipfile_name} not found or invalid")

        except FileNotFoundError as e:
            logging.error(f"Zip file {zipfile_name} not found: {e}")

        except zipfile.BadZipFile as e:
            logging.error(f"Failed to extract zip file {zipfile_name}: {e}")

        # Upload the unzipped file to the bucket
        if is_file_available:
            send_files_to_gcp(bucket, local_dir, new_file_name)


if __name__ == "__main__":
    main()
