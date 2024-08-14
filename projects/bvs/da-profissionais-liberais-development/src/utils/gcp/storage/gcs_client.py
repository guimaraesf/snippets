# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: client.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for handling objects in Google Cloud Storage
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from google.cloud import storage


class GcsClient:
    """
    This class instantiates the Google Cloud Storage client and the GCP bucket.
    """

    def __init__(self, project_id: str, bucket_id: str) -> None:
        """
        Initializes the instance with the given project ID, bucket ID, and logger.

        Args:
            project_id (str): The ID of the Google Cloud project.
            bucket_id (str): The ID of the Google Cloud Storage bucket.
        """
        self.project_id = project_id
        self.bucket_id = bucket_id

    def instantiate_client(self) -> storage.Client:
        """
        Instantiates a Google Cloud Storage client using the instance's project ID.

        Returns:
            storage.Client: A Google Cloud Storage client.
        """
        return storage.Client(self.project_id)

    def create_bucket_obj(self, client: storage.Client) -> storage.Bucket:
        """
        Creates a Google Cloud Storage bucket object using the given client and the instance's bucket ID.

        Args:
            client (storage.Client): A Google Cloud Storage client.

        Raises:
            Exception: If there is an error creating the bucket object.

        Returns:
            storage.Bucket: A Google Cloud Storage bucket object.
        """
        try:
            bucket = client.bucket(self.bucket_id)
            if bucket.exists():
                return bucket
            return None
        except Exception as e:
            raise (e)
