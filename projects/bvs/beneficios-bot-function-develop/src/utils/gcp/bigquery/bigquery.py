#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: bigquery.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for handling objects in Big Query.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
from pyspark.sql.functions import col
from google.cloud import bigquery
from google.api_core.exceptions import (
    NotFound,
    PermissionDenied,
    BadRequest,
    AlreadyExists,
)

# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))


class BigQueryClient:
    """
    The BigQuery client class.
    """

    def __init__(self) -> None:
        pass

    def instantiating_client(self):
        """
        Instantiate the BigQuery client.
        """
        return bigquery.Client()


class BigQueryUtils(BigQueryClient):
    """
    Utility functions for BigQuery.
    """

    def __init__(self, spark, project_id, dataset, logger):
        self.project_id = project_id
        self.dataset = dataset
        self.spark = spark
        self.logger = logger
        super().__init__()

    def get_target_table(self, table_name):
        """
        Returns a BigQuery table name.
        """
        return f"{self.project_id}.{self.dataset}.{table_name}"

    def __clear_table(self, schema, table_name):
        """
        Clear a BigQuery table by subscribing with an empty Dataframe.
        """
        self.logger.info(f"Clearing table data in BigQuery: {table_name}")
        df_empty = self.spark.createDataFrame([], schema=schema)
        df_empty.write.format("bigquery").mode("overwrite").option(
            "table", self.get_target_table(table_name)
        ).save()

    def __filtred_task(
        self, blob_path, start_blob_name_with, table_schema, table_name, unique_values
    ):
        """
        Cleans up tables in a BigQuery dataset.
        """
        if blob_path.startswith(start_blob_name_with):
            if table_name not in unique_values:
                unique_values.add(table_name)
                self.__clear_table(table_schema, table_name)

    def run_table_cleanup(self, task):
        """
        Cleans up tables in a BigQuery dataset.
        """
        start_blob_name_with = [
            "CNPJ",
            "SERVIDORES/ESTADUAIS/PR",
        ]

        unique_values = set()
        blob_path, table_schema, table_name = (task[1], task[4], task[6])
        for blob_name in start_blob_name_with:
            self.__filtred_task(
                blob_path, blob_name, table_schema, table_name, unique_values
            )


class BigQueryValidation(BigQueryUtils):
    """
    Validates the BigQuery table that contains the last processing date.
    """

    def __init__(self, spark, project_id, dataset, table_name, logger):
        self.spark = spark
        self.project_id = project_id
        self.dataset = dataset
        self.table_name = table_name
        self.logger = logger
        super().__init__(spark, project_id, dataset, logger)

    def get_filtred_dataframe(self, file_name, column_names):
        """
        Returns a Spark DataFrame that contains only the rows that have the specified file name.
        """
        self.logger.info("Validating last processing data in BigQuery table.")
        target_table = self.get_target_table(self.table_name)
        dataframe = (
            self.spark.read.format("bigquery")
            .option("table", target_table)
            .load()
            .select(column_names)
            .filter(col(column_names[0]) == file_name)
            .limit(1)
        )
        return dataframe

    def collect_row(self, dataframe, column_name):
        """
        Collects the content of the specified column from the given DataFrame.
        """
        row_iterator = dataframe.select(col(column_name)).collect()
        self.logger.info(
            f"This is the iterator of the row that will be archived: {row_iterator}."
        )
        return row_iterator

    @staticmethod
    def get_row_iterator_content(row_iterator, column_name_in_table):
        """
        Iterates over the rows of a BigQuery table and returns the content of the specified column.
        """
        for row in row_iterator:
            return row[column_name_in_table]

    def should_process_file(self, file_name, row_iterator_content):
        """
        Checks if the specified file name already exists in the BigQuery table.
        """
        if row_iterator_content == file_name:
            self.logger.info(
                f"File {file_name} already processed. Starting file processing."
            )
            return False
        self.logger.info(f"File {file_name} does not processed. Skipping.")
        return True

    def get_table_ref(self):
        """
        Returns a reference to the specified table in BigQuery.
        """
        target_table = self.get_target_table(self.table_name)
        client = self.instantiating_client()
        table_ref = client.get_table(target_table)
        return table_ref

    def get_table_type(self):
        """
        Returns the table type.
        """
        try:
            table_ref = self.get_table_ref()
            return table_ref.table_type
        except NotFound:
            self.logger.warning(f"BigQuery table does not exists: {self.table_name}.")
            return "EXTERNAL"


class BigQuerySave(BigQueryClient):
    """
    Saves the given DataFrame to a BigQuery table.
    """

    def __init__(self, logger) -> None:
        self.logger = logger
        self.encoding = "ISO-8859-1"
        super().__init__()

    def save_table(self, dataframe, target_table, save_mode):
        """
        Saves the given DataFrame to a BigQuery table.
        """
        try:
            self.logger.info(
                f"Saving the data to the following BigQuery table: {target_table}"
            )
            dataframe.write.format("bigquery").mode(save_mode).option(
                "table", target_table
            ).option("encoding", self.encoding).save()
            self.logger.info("Data has been saved successfully")
        except (NotFound, PermissionDenied) as gcp_error:
            self.logger.error(f"GCP Error: {gcp_error}")

    def query_external_table(self, bucket_id, blob_path, target_table):
        """
        Query external table for a given bucket and blob path and return the result.
        """
        try:
            self.logger.info("Creating External Table if it does not exist.")
            query = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS `{target_table}`
                WITH PARTITION COLUMNS
                OPTIONS (
                format = 'PARQUET',
                uris = ['gs://{bucket_id}/{blob_path}/*'],
                hive_partition_uri_prefix = 'gs://{bucket_id}/{blob_path}',
                require_hive_partition_filter = false);
            """
            return query
        except BadRequest as error:
            self.logger.error(f"BigQuery Error: {error}")
        except AlreadyExists:
            table_name = target_table.split(".")[-1]
            self.logger.warning(f"Already exists: Table {table_name}")

    def run_query_job(self, query):
        """
        Run a query job.
        """
        client = self.instantiating_client()
        query_job = client.query(query)
        return query_job

    def job_result(self, query_job):
        """
        Return a list of job results.
        """
        query_job.result()


# class BigQueryQueries(BigQueryClient):

#     def __init__(self, logger) -> None:
#         self.logger = logger
#         super().__init__()
