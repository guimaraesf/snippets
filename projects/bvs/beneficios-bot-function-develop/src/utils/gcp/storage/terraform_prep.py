#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: terraform_prep.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for preparing the paths for creating external BigQuery tables in Terraform.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
import pandas as pd
import snappy
import fastparquet

# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))
from src.utils.gcp.storage.storage import GcsClient, GcsHandle
from src.utils.config.app_log import AppLogging
from src.utils.config.variables import Variables
from src.utils.date_utils import CurrentDate


class TerraformPrep:
    def __init__(self, logger):
        self.logger = logger
        self.created_blobs = set()
        self.current_date = CurrentDate()
        self.PROJECT_ID = os.environ["PROJECT_ID"]
        self.BUCKET_ID = os.environ["BUCKET_ID"]
        self.file_path = os.path.join(os.getcwd(), "tf.parquet")
        self.gcs_client = GcsClient(self.PROJECT_ID, self.BUCKET_ID, self.logger)
        self.bucket = self.gcs_client.create_bucket_object()
        self.gcs_handle = GcsHandle(self.bucket, self.logger)

    def get_date(self):
        today = self.current_date.get_today()
        return (today.strftime(fmt) for fmt in ["%Y", "%m", "%d"])

    def create_and_save_df(self):
        df = pd.DataFrame()
        df.to_parquet(self.file_path)

    def upload_blob(self, blob, blob_path, file_path):
        if not blob.exists() and blob_path not in self.created_blobs:
            self.gcs_handle.blob_upload_from_file(blob, file_path)
            self.logger.info(f"Blob has been created: {blob}")
        self.logger.info(f"Blob already exists: {blob}")

    def create_blob(self, year, month, day):
        for blob_path in Variables.TRUSTED_BLOB_PATHS:
            blob_name = f"{blob_path}/ANO={year}/MES={month}/DIA={day}/tf.parquet"
            blob = self.gcs_handle.create_blob(blob_name)
            self.upload_blob(blob, blob_path, self.file_path)
            self.created_blobs.add(blob_path)

    def run(self):
        try:
            year, month, day = self.get_date()
            self.create_and_save_df()
            self.create_blob(year, month, day)
        except Exception as exception:
            self.logger.error(f"An error occurred: {exception}")
            raise


if __name__ == "__main__":
    logger = AppLogging()
    app = TerraformPrep(logger)
    app.run()
