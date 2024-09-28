#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: storage.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for handling objects in Google Cloud Storage
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
from google.cloud import storage
from pyspark.sql.functions import col

# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))


class GcsClient:
    """
    This class instantiates the Google Cloud Storage client and the GCP bucket.
    """

    def __init__(self, project_id, bucket_id, logger):
        self.project_id = project_id
        self.bucket_id = bucket_id
        self.logger = logger

    def instantiating_client(self):
        """
        Instantiates the Google Cloud Storage client.
        :return: The Google Cloud Storage client.
        """
        return storage.Client(self.project_id)

    def create_bucket_object(self):
        """
        Instantiates the GCP bucket.
        :param client: The Google Cloud Storage client.
        :return: The GCP bucket.
        """
        self.logger.info(
            f"Configuring the application to access the GCP bucket: {self.bucket_id}"
        )
        client = self.instantiating_client()
        return client.bucket(self.bucket_id)


class GcsHandle:
    """
    Class to interact with Google Cloud Storage.
    """

    def __init__(self, bucket, logger) -> None:
        self.bucket = bucket
        self.logger = logger

    @staticmethod
    def delete_blob(blob):
        """
        Deletes a blob.
        """
        blob.delete()

    @staticmethod
    def replace_blob(blob, source_path, destination_path):
        """
        Replaces a blob.
        """
        return blob.name.replace(source_path, destination_path)

    def get_list_blobs(self, prefix=None):
        """
        Returns a list of blobs in a bucket.
        """
        return list(self.bucket.list_blobs(prefix=prefix))

    def move_file(self, blob, source_path, destination_path):
        """
        Moves a file in GCS.
        """
        self.logger.info("Moving file to the backup bucket.")
        backup = self.replace_blob(blob, source_path, destination_path)
        self.bucket.copy_blob(blob, self.bucket, backup)

    def download_blob(self, source_path, blob, file_name):
        """
        Downloads a blob from GCS.
        """
        self.logger.info(
            f"Downloading the following file from the GCP bucket: {blob.name}"
        )
        file_path = os.path.join(source_path, file_name)
        blob.download_to_filename(file_path)
        return file_path

    @staticmethod
    def blob_upload_from_file(blob, file_path):
        """
        Processes a blob.
        """
        with open(file_path, "rb") as file:
            blob.upload_from_file(file, content_type="application/zip")

    def create_blob(self, blob_path):
        """
        Create a blob object from the given file name.
        """
        blob = self.bucket.blob(blob_path)
        return blob

    def get_filtred_dataframe(self, spark, file_path, file_name):
        """
        Returns a Spark DataFrame that contains only the column that have the specified file name.
        """
        try:
            dataframe = (
                spark.read.parquet(file_path)
                .select("ARQUIVO")
                .filter(col("ARQUIVO") == file_name)
                .limit(1)
            )
            return dataframe
        except Exception:
            pass

    def should_process_file(self, dataframe, file_name):
        """
        Checks if the specified file name already exists in the GCS.
        """
        if dataframe is None or dataframe.rdd.isEmpty():
            self.logger.info(
                f"File {file_name} does not processed. Starting processing file."
            )
            return True
        self.logger.info(f"File {file_name} already processed. Skipping.")
        return False

    def write_to_parquet(self, dataframe, destination_path, mode):
        """
        Writes the given dataframe to the given destination.
        """
        try:
            blob_trusted_name = "/".join(destination_path.split("/")[3:])
            dataframe.write.mode(mode).parquet(destination_path)
            self.logger.info(
                f"DataFrame was successfully written to the Trusted layer: {blob_trusted_name}."
            )
        except Exception as error:
            self.logger.error(f"An error occurred while writing the DataFrame: {error}")

    def upload_blobs_to_gcs(self, source_path, file_names, file_blob_map):
        """
        Upload blobs from files to GCS.
        """
        try:
            for file in file_names:
                file_path = os.path.join(source_path, file)
                if os.path.exists(file_path):
                    blob_path = os.path.join(file_blob_map.get(file), file)
                    blob = self.create_blob(blob_path)
                    self.blob_upload_from_file(blob, file_path)
                    self.logger.info(f"File were uploaded successfully to GCS: {blob}")
        except (FileNotFoundError, KeyError, AttributeError) as error:
            self.logger.error(f"An error occurred while uploading the file: {error}")


class GcsFileValidator:
    """
    Class for validating files in GCS.
    """

    def __init__(self):
        pass

    def file_exists_in_bucket(self, blobs, file_names, logger):
        """
        Validates if a file already exists in the bucket.
        """
        logger.info("Checking if the file already exists in the bucket")
        for file_name in file_names:
            found = False
            for blob in blobs:
                if blob.name.endswith(file_name):
                    logger.info(f"File {file_name} already exists in the bucket.")
                    found = True
                    break
            if not found:
                logger.info(f"File {file_name} does not exist in the bucket.")
                return False
        return True
