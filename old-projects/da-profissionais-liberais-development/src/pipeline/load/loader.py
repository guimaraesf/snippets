# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: loader.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for executing the ingestion task.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from __future__ import annotations

import concurrent.futures
import importlib
import os
import sys

from google.cloud import storage
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


# When run in Dataproc workspace the original directory structure is not preserved.
# These imports are useful for the code to be executed both locally and in another external environment.
PATH = os.path.dirname(os.path.abspath("__file__"))
sys.path.append(PATH)


def get_module_path(root_path: str):
    """
    This function returns the module path based on the given root path.

    Args:
    root_path (str): The root path of the module.

    Returns:
    str: If PATH does not start with sys.argv[9], it returns the root path.
    """
    if not PATH.startswith(sys.argv[9]):
        return root_path
    else:
        return root_path.split(".")[-1]


BUILDER_MOD_PATH = get_module_path("src.utils.dataframe.pyspark.schema.builder")
COLUMNS_MOD_PATH = get_module_path("src.utils.dataframe.pyspark.schema.columns")
FILE_CLEANER_MOD_PATH = get_module_path("src.utils.dataframe.pyspark.processors.file_cleaner")
GCS_CLIENT_MOD_PATH = get_module_path("src.utils.gcp.storage.gcs_client")
GCS_MANAGER_MOD_PATH = get_module_path("src.utils.gcp.storage.gcs_manager")
LOGGER_MOD_PATH = get_module_path("src.utils.helpers.logger")
PYSPARK_HANDLER_MOD_PATH = get_module_path("src.utils.dataframe.pyspark.processors.file_operator")
SPARKLAUNCHER_MOD_PATH = get_module_path("src.utils.spark.sparksession")
STOPWATCH_MOD_PATH = get_module_path("src.utils.time.stopwatch")
UTILS_MOD_PATH = get_module_path("src.utils.helpers.utils")
DATES_MOD_PATH = get_module_path("src.utils.time.dates")
VARIABLES_MOD_PATH = get_module_path("src.utils.helpers.variables")
ILOAD_MOD_PATH = get_module_path("src.interfaces.interface_loader")

Columns = importlib.import_module(COLUMNS_MOD_PATH).Columns
GcsClient = importlib.import_module(GCS_CLIENT_MOD_PATH).GcsClient
GcsManager = importlib.import_module(GCS_MANAGER_MOD_PATH).GcsManager
Logger = importlib.import_module(LOGGER_MOD_PATH).Logger
SparkLauncher = importlib.import_module(SPARKLAUNCHER_MOD_PATH).SparkLauncher
SchemaTables = importlib.import_module(BUILDER_MOD_PATH).SchemaTables
CleanTables = importlib.import_module(FILE_CLEANER_MOD_PATH).CleanTables
PysparkHandler = importlib.import_module(PYSPARK_HANDLER_MOD_PATH).PysparkHandler
Stopwatch = importlib.import_module(STOPWATCH_MOD_PATH).Stopwatch
UtilsDataproc = importlib.import_module(UTILS_MOD_PATH).UtilsDataproc
Dates = importlib.import_module(DATES_MOD_PATH).Dates
Variables = importlib.import_module(VARIABLES_MOD_PATH).Variables
ILoader = importlib.import_module(ILOAD_MOD_PATH).ILoader


class Loader(ILoader, SchemaTables, CleanTables, PysparkHandler):
    """
    A class that loads data using schemas and tables, handles PySpark operations, and cleans tables.
    """

    def __init__(
        self,
        columns=Columns,
        logger_obj=Logger,
        gcs_client=GcsClient,
        gcs_manager=GcsManager,
        spark_launcher=SparkLauncher,
        utils_dataproc=UtilsDataproc,
        dates=Dates,
        variables=Variables,
    ) -> None:
        """
        Initializes the Transformer with the given parameters.

        Args:
            columns (Columns, optional): The columns to be used in the transformation. Defaults to Columns.
            logger_obj (Logger, optional): An instance of the Logger class for logging. Defaults to Logger.
            gcs_client (GcsClient, optional): A Google Cloud Storage client. Defaults to GcsClient.
            gcs_manager (GcsManager, optional): A manager for Google Cloud Storage operations. Defaults to GcsManager.
            spark_launcher (SparkLauncher, optional): A launcher for Spark operations. Defaults to SparkLauncher.
            utils_dataproc (UtilsDataproc, optional): Utilities for Google Cloud Dataproc. Defaults to UtilsDataproc.
            dates (Dates, optional): The current date. Defaults to Dates.
            variables (Variables, optional): The variables to be used in the transformation. Defaults to Variables.
        """
        SchemaTables.__init__(self)
        CleanTables.__init__(self)
        PysparkHandler.__init__(self)
        self.logger = logger_obj()
        self.columns = columns
        self.gcs_client = gcs_client
        self.gcs_manager = gcs_manager
        self.spark_launcher = spark_launcher
        self.utils_dataproc = utils_dataproc()
        self.dates = dates()
        self.variables = variables
        self.transient_layer = self.variables.TRANSIENT_NAME
        self.trusted_layer = self.variables.TRUSTED_NAME

    @property
    def get_schema(self) -> StructType:
        """
        Gets the schema.

        Returns:
            StructType: Description of the return value.
        """
        return self.schema

    def init_sparksession(
        self, master: str, app_name: str, bucket_id: str, dataset_id: str, project_id: str
    ) -> SparkSession:
        """
        Initializes the Spark session.

        Args:
            master (str): The master URL of the Spark application.
            app_name (str): The name of the Spark application.
            bucket_id (str): The bucket_id in which the results of the Spark application will be stored.
            dataset_id (str): The dataset that the Spark application will process.
            project_id (str): The ID of the project in which the Spark application is running.

        Returns:
            SparkSession: Description of the return value.
        """
        spark_launcher = self.spark_launcher(master, app_name, bucket_id, dataset_id, project_id)
        spark = spark_launcher.initialize_sparksession()
        return spark

    def get_bucket_obj(self, project_id: str, bucket_id: str) -> storage.Bucket:
        """
        Gets the bucket object.

        Args:
            project_id (str): Description of the project_id parameter.
            bucket_id (str): Description of the bucket_id parameter.

        Returns:
            storage.Bucket: Description of the return value.
        """
        gcs_client = self.gcs_client(project_id, bucket_id)
        client = gcs_client.instantiate_client()
        bucket = gcs_client.create_bucket_obj(client)
        return bucket

    def get_list_blobs(self, bucket_obj: storage.Bucket, dir_name: str) -> list[str]:
        """
        Gets the list of blobs.

        Args:
            bucket_obj (storage.Bucket): Description of the bucket parameter.
            dir_name (str): Description of the dir_name parameter.

        Returns:
            list[str]: Description of the return value.
        """
        gcs_manager = self.gcs_manager(bucket_obj)
        blobs = gcs_manager.get_list_blobs(dir_name)
        return [blob.name for blob in blobs]

    def df_is_valid(self, df) -> bool:
        """
        Checks if the DataFrame is not empty.

        Args:
            df (pyspark.sql.DataFrame): The DataFrame to check.

        Returns:
            bool: Returns True if the DataFrame is not empty, otherwise it logs an error and returns False.
        """
        if not df.rdd.isEmpty():
            return True
        self.logger.error(f"The DataFrame is empty: {df.show()}")
        return False

    def build_df_schema(self, table_infos: dict) -> None:
        """
        Builds the dataframe schema.

        Args:
            table_infos (dict): Description of the table_infos parameter.
        """
        # Creating spark schemas from columns.
        self.create_spark_struct_type(table_infos)
        # Adding control columns to the schema.
        self.create_spark_struct_type(self.columns.INFO_CONTROL_COLUMNS)

    def process_blob(
        self,
        spark: SparkSession,
        bucket_id: str,
        blob_name: str,
        df_params: tuple,
    ) -> None:
        """
        Process a file from blob.

        Args:
            spark (SparkSession): The SparkSession to use.
            bucket_id (str): The ID of the bucket where the blobs are located.
            blob_name (str): The blob name to transform.
            df_params (tuple): Information about the dataframes.
        """
        write_format, delimiter, header, encoding, mode = df_params
        if "part" in blob_name:
            source_path = f"gs://{bucket_id}/{blob_name}"
            # Read CSV file in transient layer.
            df = self.read_csv_file(
                spark, source_path, delimiter, header, encoding, self.get_schema
            )
            if self.df_is_valid(df):
                # Write a blob into transient layer.
                dest_path = source_path.replace(self.transient_layer, self.trusted_layer)
                self.write_file(df, write_format, dest_path, mode)

    def process_all_blobs(
        self,
        spark: SparkSession,
        bucket_id: str,
        blob_names: list,
        df_params: tuple,
    ) -> None:
        """
        Processes all blobs in a specific bucket.

        This function uses multithreading to process multiple blobs simultaneously.

        Args:
            spark (SparkSession): The Spark session to execute DataFrame operations.
            bucket_id (str): The ID of the Google Cloud Storage bucket where the blobs are located.
            blob_names (list): A list of blob names to process.
            df_params (tuple): A set of parameters to pass to the DataFrame creation function.

        Returns:
            None
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.process_blob, spark, bucket_id, blob, df_params)
                for blob in blob_names
            ]
        future_as_completed = list(concurrent.futures.as_completed(futures))
        if len(future_as_completed) == len(blob_names):
            self.logger.info("All incoming blobs have been processed.")

    def check_before_processing(self, blob_names: list) -> None:
        """
        Check if exists blob names before to process.

        Args:
            blob_names (list): A list of blob names to process.
        """
        if blob_names:
            self.logger.info(f"{len(blob_names)} files were found to process.")
            return True
        self.logger.warning("No blobs were found.")
        return False

    def run_load(
        self,
        step_id: str,
        bucket_id: str,
        dataset_id: str,
        project_id: str,
        app_name: str,
        master: str,
    ) -> None:
        """
        The main method of the Loader class.
        """
        infos = self.variables.LOADER_TASKS.get(step_id)

        dir_name, table_infos, df_params = (infos.dir_name, infos.table_infos, infos.df_params)

        spark = self.init_sparksession(master, app_name, bucket_id, dataset_id, project_id)

        sc = SparkConf()
        self.logger.info(f"Spark Configuration: {sc.getAll()}")

        self.logger.info("Building Spark Schema for DataFrames.")
        self.build_df_schema(table_infos)

        bucket = self.get_bucket_obj(project_id, bucket_id)
        YEAR, MONTH, DAY = self.dates.get_dates()
        self.logger.info(
            f"Searching files to process in: {self.transient_layer}/{dir_name}/ano={YEAR}/mes={MONTH}"
        )
        blob_names = self.get_list_blobs(
            bucket, f"{self.transient_layer}/{dir_name}/ano={YEAR}/mes={MONTH}"
        )
        if self.check_before_processing(blob_names):
            self.logger.info(
                f"Running data transformations for Blob: {self.trusted_layer}/{dir_name}/ano={YEAR}/mes={MONTH}/dia={DAY}"
            )
            self.process_all_blobs(spark, bucket_id, blob_names, df_params)

        spark.stop()
        self.logger.info("The SparkSession has been stopped.")


if __name__ == "__main__":
    logger = Logger()

    stopwatch = Stopwatch()
    stopwatch.start()

    logger.info("Starting transformation process.")
    utils_dataproc = UtilsDataproc()

    STEP_ID, BUCKET_ID, DATASET_ID, PROJECT_ID, APP_NAME, MASTER = (
        utils_dataproc.get_step_id,
        utils_dataproc.get_bucket_id,
        utils_dataproc.get_dataset_id,
        utils_dataproc.get_project_id,
        utils_dataproc.get_spark_app_name,
        utils_dataproc.get_spark_master,
    )

    load = Loader()
    load.run_load(STEP_ID, BUCKET_ID, DATASET_ID, PROJECT_ID, APP_NAME, MASTER)

    stopwatch.stop()
    stopwatch.show_elapsed_time()
