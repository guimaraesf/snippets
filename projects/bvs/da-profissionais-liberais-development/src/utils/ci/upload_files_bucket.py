# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: upload_files_bucket.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for preparing the bucket partitions to create external tables via Terraform
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import os
from datetime import date

import pandas as pd
from google.cloud import storage


class Uploader:
    """
    This class is responsible for uploading files to Google Cloud Storage.
    """

    def __init__(self, bucket: storage.Bucket) -> None:
        """
        Initializes the Uploader class with the given project ID, bucket ID, and logger.

        Args:
            bucket (storage.Bucket): The bucket object.
        """
        self.bucket = bucket
        self.file_path = os.path.join(os.getcwd(), "tf.parquet")
        self.blob_trusted_name = (
            "conselho-federal-contabilidade",
            "conselho-federal-fonoaudiologia",
            # "conselho-federal-educacao-fisica",
            # "superintendencia-seguros-privados",
        )

    def create_empty_df(self) -> None:
        """
        Creates an empty DataFrame and saves it as a Parquet file.
        """
        df = pd.DataFrame()
        df.to_parquet(self.file_path)

    def upload_parquet_to_trusted(self, current_dates: tuple) -> None:
        """
        Uploads a parquet file to a trusted blob path.

        Args:
            current_dates (tuple): The current dates in the format (year, month, day).
        """
        YEAR, MONTH, DAY = current_dates
        for blob_path in self.blob_trusted_name:
            blob_trusted_path = f"trusted/{blob_path}/ano={YEAR}/mes={MONTH}/dia={DAY}/tf.parquet"
            try:
                blob = self.bucket.blob(blob_trusted_path)
                blob.upload_from_filename(self.file_path, content_type="application/octet-stream")
                if blob.exists():
                    print(f"{blob.name} has been uploaded successfully.")
            except Exception as e:
                print(e)


if __name__ == "__main__":
    BUCKET_ID, PROJECT_ID = (os.getenv(var) for var in ("BUCKET_ID", "PROJECT_ID"))
    client = storage.Client(PROJECT_ID)
    bucket = client.bucket(BUCKET_ID)

    uploader = Uploader(bucket)
    # Create empty DataFrame for create of external tables
    uploader.create_empty_df()

    print("Uploading python files for Trusted in Cloud Storage")
    dates = (date.today().strftime(fmt) for fmt in ("%Y", "%m", "%d"))
    uploader.upload_parquet_to_trusted(dates)

    print("All files have been uploaded successfully.")
