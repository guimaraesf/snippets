#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: test_storage.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This  ..code.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
import shutil
import unittest
from unittest.mock import MagicMock, patch
from google.cloud import storage
# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))
from src.utils.gcp.storage.storage import GcsClient, GcsHandle


class GcsHandleTest(unittest.TestCase):
    """
    Test cases for the GcsHandle class.
    """
    def setUp(self):
        self.project_id = os.getenv("PROJECT_ID")
        self.bucket_id = os.getenv("BUCKET_ID")
        self.test_dir = os.path.join(os.getcwd(), "/src/tests/integration/test_dir")
        self.test_file_path = os.path.join(self.test_dir, "test.txt")
        self.blob_name = "test-blob"
        self.logger = MagicMock()
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
    
    def test_delete_blob(self):
        """
        Test deleting a blob from the bucket.
        """
        # Arrange
        client = storage.Client(self.project_id)
        bucket = client.bucket(self.bucket_id)
        blob = bucket.blob(self.blob_name)

        os.makedirs(self.test_dir, exist_ok=True)
        with open(self.test_file_path, "w", encoding="UTF-8") as file:
            file.write("")

        blob.upload_from_filename(self.test_file_path)

        # Act
        gcs_handle = GcsHandle(self.bucket_id, self.logger)
        gcs_handle.delete_blob(blob)

        # Assert
        self.assertFalse(blob.exists())


class GcsClientTest(unittest.TestCase):
    """
    Test cases for the GcsClient class.
    """

    def setUp(self):
        self.project_id = "my-project-id"
        self.bucket_id = "my-bucket-id"
        self.logger = MagicMock()

    def test_instantiating_client(self):
        """
        Test that the Google Cloud Storage client is instantiated correctly.
        """
        # Act
        gcs_client = GcsClient(self.project_id, self.bucket_id, self.logger)
        client = gcs_client.instantiating_client()

        # Assert
        self.assertIsInstance(client, storage.Client)
        self.assertEqual(client.project, self.project_id)

    def test_create_bucket_object(self):
        """
        Test that the GCP bucket is instantiated correctly.
        """
        # Act
        gcs_client = GcsClient(self.project_id, self.bucket_id, self.logger)
        bucket = gcs_client.create_bucket_object()

        # Assert
        self.assertIsInstance(bucket, storage.Bucket)
        self.assertEqual(bucket.name, self.bucket_id)


if __name__ == "__main__":
    unittest.main()
