# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: gcs_manager.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for handling objects in Google Cloud Storage
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from google.cloud import storage
from typing import Optional


class GcsManager:
    """
    Manages operations related to Google Cloud Storage.
    """

    def __init__(self, bucket_obj: storage.Bucket) -> None:
        """
        Initializes the GcsManager class.

        Args:
            bucket (storage.Bucket): A Google Cloud Storage bucket object.
        """
        self.bucket = bucket_obj

    def get_list_blobs(self, prefix: Optional[str] = None) -> list:
        """
        Gets a list of blobs in the bucket that match the given prefix.

        Args:
            prefix (str, optional): The prefix to match. Defaults to None.

        Returns:
            list: A list of blobs that match the prefix.
        """
        return list(self.bucket.list_blobs(prefix=prefix))

    @staticmethod
    def blob_upload_from_file(blob: storage.Blob, source_path: str) -> None:
        """
        Uploads a file to a blob in Google Cloud Storage.

        Args:
            blob (object): The blob object where the file will be uploaded.
            source_path (str): The path of the file to be uploaded.
        """
        with open(source_path, "rb") as file:
            blob.upload_from_file(file, content_type="application/octet-stream")

    @staticmethod
    def blob_upload_from_string(blob: storage.Blob, data: str) -> None:
        """
        Performs an operation on the given blob with the provided data.

        Args:
            blob (storage.Blob): The blob to operate on.
            data (str): The data to use in the operation.
        """
        blob.upload_from_string(data, content_type="application/octet-stream")

    @staticmethod
    def blob_upload_from_file_name(blob: storage.Blob, data: str) -> None:
        """
        Performs an operation on the given blob with the provided data.

        Args:
            blob (storage.Blob): The blob to operate on.
            data (str): The data to use in the operation.
        """
        blob.upload_from_filename(data, content_type="application/octet-stream")

    def create_blob(self, blob_path: str) -> storage.Blob:
        """
        Creates a new blob in the bucket.

        Args:
            blob_path (str): The path where the new blob will be created.

        Returns:
            storage.Blob: The newly created blob object.
        """
        return self.bucket.blob(blob_path)

    def check_if_blob_exists(self, blob: storage.Blob) -> bool:
        """
        Checks if the given blob exists.

        Args:
            blob (storage.Blob): The blob to check.

        Returns:
            bool: True if the blob exists, False otherwise.
        """
        if blob.exists():
            return True
        return False
