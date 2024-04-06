#!/usr/bin/env python
# -*- coding: utf-8 -*-


# ================================================================================================
# Module: main_cronjob_servidores.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code captures and stores data on remuneration of Federal Public Servants
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
import zipfile
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
        f"{date}_Aposentados_BACEN.csv": "SERVIDORES/APOSENTADOS/BACEN/ENTRADA",
        f"{date}_Aposentados_SIAPE.csv": "SERVIDORES/APOSENTADOS/SIAPE/ENTRADA",
        f"{date}_Honorarios_Advocaticios.csv": "SERVIDORES/HONORARIOSADVOC/ENTRADA",
        f"{date}_Honorarios_Jetons.csv": "SERVIDORES/HONORARIOSJETONS/ENTRADA",
        f"{date}_Militares.csv": "SERVIDORES/MILITARES/ENTRADA",
        f"{date}_Pensionistas_BACEN.csv": "SERVIDORES/PENSIONISTAS/BACEN/ENTRADA",
        f"{date}_Pensionistas_SIAPE.csv": "SERVIDORES/PENSIONISTAS/SIAPE/ENTRADA",
        f"{date}_Pensionistas_DEFESA.csv": "SERVIDORES/PENSIONISTAS/DEFESA/ENTRADA",
        f"{date}_Reserva_Reforma_Militares.csv": "SERVIDORES/RESERVAMILIARES/ENTRADA",
        f"{date}_Servidores_BACEN.csv": "SERVIDORES/SERVPUBLICOS/BACEN/ENTRADA",
        f"{date}_Servidores_SIAPE.csv": "SERVIDORES/SERVPUBLICOS/SIAPE/ENTRADA",
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
        with urllib.request.urlopen(url) as response, open(save_path, "wb") as outfile:
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


FILES_NAMES = {
    "Aposentados_BACEN": "Aposentados_BACEN",
    "Aposentados_SIAPE": "Aposentados_SIAPE",
    "Honorarios_Advocaticios": "Honorarios_Advocaticios",
    "Honorarios_Jetons": "Honorarios_Jetons",
    "Militares": "Militares",
    "Pensionistas_BACEN": "Pensionistas_BACEN",
    "Pensionistas_DEFESA": "Pensionistas_DEFESA",
    "Pensionistas_SIAPE": "Pensionistas_SIAPE",
    "Reserva_Reforma_Militares": "Reserva_Reforma_Militares",
    "Servidores_BACEN": "Servidores_BACEN",
    "Servidores_SIAPE": "Servidores_SIAPE",
}


def get_files_names(file):
    return FILES_NAMES.get(file, file)


def extract_and_rename_file(local_dir, dt_refer, zipfile_path, file, new_file_name):
    UnzipFiles.extract_all(local_dir, zipfile_path)

    old_file_path = os.path.join(local_dir, file)
    new_file_path = os.path.join(local_dir, f"{dt_refer}_{new_file_name}.csv")
    os.rename(old_file_path, new_file_path)

    logging.info(f"Renamed {old_file_path} to {new_file_path}")


class UnzipFiles:
    @staticmethod
    def extract_files(extract_path: str, file_path: str, files: list) -> None:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            for file in files:
                zip_ref.extract(member=file, path=extract_path)

            logging.info(f"All files extracted from {file_path} to {extract_path}.")

    @staticmethod
    def extract_all(extract_path: str, file_path: str) -> None:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)
            logging.info(f"All files extracted from {file_path} to {extract_path}.")


def main():
    logging.basicConfig(level=logging.INFO)
    bucket_dir = sys.argv[2]  # i.e.: hml-gcp-dados-alternativos    
    project_id = sys.argv[3]  # i.e.: data-88d7
    local_dir = sys.argv[4]
    bucket = configure_storage(project_id, bucket_dir)

    files = [
        "Aposentados_BACEN",
        "Aposentados_SIAPE",
        "Honorarios_Advocaticios",
        "Honorarios_Jetons",
        "Militares",
        "Pensionistas_BACEN",
        "Pensionistas_DEFESA",
        "Pensionistas_SIAPE",
        "Reserva_Reforma_Militares",
        "Servidores_BACEN",
        "Servidores_SIAPE",
    ]

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

        for file in files:
            url = f"https://www.portaltransparencia.gov.br/download-de-dados/servidores/{dt_refer}_{file}"
            save_path = os.path.join(local_dir, f"{dt_refer}_{file}.zip")
            file_name = f"{dt_refer}_{get_files_names(file)}"
            table_name = file.replace("_", " ")

            is_file_available = False

            logging.info(
                f"Starting the download for the Federal Public Servants {table_name} from {dt_name}."
            )
            # Check that the downloaded file exists
            if validate_existing_file(bucket, file_name):
                download_url(url, save_path)

                try:
                    zipfile_name = f"{dt_refer}_{file}.zip"
                    zipfile_path = os.path.join(local_dir, zipfile_name)

                    if os.path.exists(zipfile_path) and zipfile.is_zipfile(
                        zipfile_path
                    ):
                        # Honorarios_Advocaticios
                        if file == files[2]:
                            extract_and_rename_file(
                                local_dir,
                                dt_refer,
                                zipfile_path,
                                f"{dt_refer}_HonorariosAdvocaticios.csv",
                                files[2],
                            )
                            # Upload the unzipped file to the bucket
                            send_files_to_gcp(bucket, local_dir, dt_refer)

                        # Honorarios_Jetons
                        elif file == files[3]:
                            extract_and_rename_file(
                                local_dir,
                                dt_refer,
                                zipfile_path,
                                f"{dt_refer}_Honorarios(Jetons).csv",
                                files[3],
                            )
                            # Upload the unzipped file to the bucket
                            send_files_to_gcp(bucket, local_dir, dt_refer)

                        # Other files
                        elif file in files:
                            # Change the directory of "dataproc job" to "/tmp"
                            os.chdir("../")
                            extracted_files = [
                                f"{dt_refer}_Cadastro.csv",
                                f"{dt_refer}_Remuneracao.csv",
                            ]

                            # Unzip the files Cadastro.csv and Remuneracao.csv
                            UnzipFiles.extract_files(
                                local_dir, zipfile_path, extracted_files
                            )

                            cadastro_file = os.path.join(local_dir, extracted_files[0])
                            remuneracao_file = os.path.join(
                                local_dir, extracted_files[1]
                            )

                            logging.info("Reading CSV files into DataFrames.")
                            df_cadastro = pd.read_csv(
                                cadastro_file,
                                sep=";",
                                encoding="ISO-8859-1",
                                low_memory=False,
                            )
                            df_remuneracao = pd.read_csv(
                                remuneracao_file,
                                sep=";",
                                encoding="ISO-8859-1",
                                low_memory=False,
                            )

                            logging.info(
                                "Merging the files Cadastro and Remuneração DataFrames."
                            )
                            merged_df = df_cadastro.merge(
                                df_remuneracao,
                                how="inner",
                                on=["Id_SERVIDOR_PORTAL", "NOME", "CPF"],
                            )

                            # Rename the merged csv file with the new name
                            merged_file_name = os.path.join(
                                local_dir, f"{dt_refer}_{file}.csv"
                            )
                            merged_df.to_csv(
                                merged_file_name,
                                sep=";",
                                encoding="ISO-8859-1",
                                index=False,
                            )

                            logging.info(
                                f"Files successfully merged and saved as csv to: {merged_file_name}"
                            )

                            # Upload the unzipped file to the bucket
                            send_files_to_gcp(bucket, local_dir, dt_refer)

                except FileNotFoundError as e:
                    logging.error(f"File {file} not found: {e}")

                except zipfile.BadZipFile as e:
                    logging.error(f"Failed to extract zip file {file}: {e}")

    logging.info("Cronjob successfully finished")


if __name__ == "__main__":
    main()
