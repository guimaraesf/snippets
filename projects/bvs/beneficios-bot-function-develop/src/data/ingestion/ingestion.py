#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: ingestion.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code ingests data from processes that run.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
from pyspark.sql.functions import *
from pyspark import SparkContext
# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))
from task import IngestionTasks
from app_log import AppLogging
from spark import SparkLauncher
from variables import Variables
from storage import GcsClient, GcsHandle
from bigquery import BigQueryValidation, BigQuerySave, BigQueryUtils
from date_utils import (
    DateUtils,
    CurrentDate,
    DailyDateHandle,
    MonthlyDateHandle,
    QuarterDateHandle,
    YearlyDateHandle
)
from utils import (
    EnvironmentUtils, 
    HdfsUtils, 
    FileUtils, 
    SqlUtils, 
    DataframeUtils,
    Stopwatch
)


class DataUtils:
    """
    Helper class to process data.
    """
    def __init__(self, spark, project_id, bucket_id, dataset, local_dir, logger):
        self.spark = spark
        self.project_id = project_id
        self.bucket_id = bucket_id
        self.dataset = dataset
        self.local_dir = local_dir
        self.logger = logger

    def get_tasks_for_ingestions(self, env_vars):
        """
        Returns a dict of tasks for ingestion.
        """
        ingestion_tasks = IngestionTasks(env_vars)
        tasks_for_ingestion = ingestion_tasks.get_ingestion_tasks()
        return tasks_for_ingestion

    def clearing_bigquery_tables(self, task):
        """
        Clears specific bigquery tables.
        """
        bigquery_utils = BigQueryUtils(self.spark, self.project_id, self.dataset, self.logger)
        bigquery_utils.run_table_cleanup(task)

    def get_target_date(self, daily_date_handle, monthly_date_handle, quarter_date_handle, year_date_handle, blob_path, period):
        """
        Getting target date for processing.
        """
        date_utils = DateUtils(daily_date_handle, monthly_date_handle, quarter_date_handle, year_date_handle)
        target_date = date_utils.get_target_date(blob_path, period)
        return target_date

    def get_target_date_args(self, blob_path, target_date):
        """
        Returns a list of target date arguments.
        """
        file_utils = FileUtils()
        target_date_arg = file_utils.check_blob_path(blob_path, target_date)
        return target_date_arg

    def get_file_name_for_processing(self, snippet_name, target_date_arg):
        """
        Setting file name for processing.
        """
        file_utils = FileUtils()
        file_name = file_utils.get_file_names(snippet_name, self.logger, **target_date_arg)
        return file_name

    def get_target_table(self, table_name):
        """
        Returns the target table for the given file.
        """
        bigquery_utils = BigQueryUtils(self.spark, self.project_id, self.dataset, self.logger)
        target_table = bigquery_utils.get_target_table(table_name)
        return target_table

    def get_list_blobs(self, bucket, blob_path):
        """
        Listing existing blobs based on prefix.
        """
        gcs_handle = GcsHandle(bucket, self.logger)
        blobs = gcs_handle.get_list_blobs(blob_path)
        return blobs

    def delete_temporary_files(self):
        """
        Delete temporary files.
        """
        file_utils = FileUtils()
        file_utils.delete_temporary_files(self.local_dir, self.logger)

    def get_task_details(self, task):
        """
        Returns the details of a task.
        """
        file_utils = FileUtils()
        task_details = file_utils.get_task_details(task)
        return task_details

    @staticmethod
    def get_uri_gcs(bucket_id, blob_path, year, month, day):
        """
        Getting URI name for processing.
        """
        return f"gs://{bucket_id}/{blob_path}/ANO={year}/MES={month}/DIA={day}/"

    @staticmethod
    def get_tag(uri):
        """
        Get the tag associated with the given uri.
        """
        return "/".join(uri.split("/")[3:-2])

    def deleting_files_in_gcs(self, bucket, list_blobs):
        """
        Deleting files stored at earlier dates.
        """
        blobs_to_delete_before_process = ["SERVIDORES/ESTADUAIS/TRUSTED/DF/PATRIMONIO"]
        gcs_handle = GcsHandle(bucket, self.logger)
        for blob in list_blobs:
            for path in blobs_to_delete_before_process:
                if blob.exists() and blob.name.startswith(path) and blob.name.endswith("snappy.parquet"):
                    gcs_handle.delete_blob(blob)
                    self.logger.warning(f"Deleting blob at earlier dates: {blob}.")


class DataConfig:
    """
    Getting parameters of configuration.
    """
    def __init__(self, project_id, bucket_id, dataset, app_name, master, logger):
        self.project_id = project_id
        self.bucket_id = bucket_id
        self.dataset = dataset
        self.app_name = app_name
        self.master = master
        self.logger = logger

    def get_sparksession(self):
        """
        Instantiating SparkSession.
        """
        spark_launcher = SparkLauncher(self.master, self.app_name, self.project_id, self.dataset, self.bucket_id)
        spark = spark_launcher.initialize_sparksession()
        return spark

    def instantiate_bucket(self):
        """
        Configure Client Cloud Storage.
        """
        gcs_client = GcsClient(self.project_id, self.bucket_id, self.logger)
        bucket = gcs_client.create_bucket_object()
        return bucket


class DataValidate:
    """ 
    This class is used to validate the data against the specified target table.
    """
    def __init__(self, spark, project_id, bucket_id, dataset, table_name, logger):
        self.spark = spark
        self.project_id = project_id
        self.bucket_id = bucket_id
        self.dataset = dataset
        self.table_name = table_name
        self.logger = logger
        self.bq_validation = BigQueryValidation(self.spark, self.project_id, self.dataset, self.table_name, self.logger)

    def validate_file_in_bq(self, file_name) -> bool:
        """ 
        Returns TRUE if the file does not exists in BigQuery table.
        """
        columns_to_validate = ["ARQUIVO", "DATA_PROCESSAMENTO"]
        filtred_df = self.bq_validation.get_filtred_dataframe(file_name, columns_to_validate)
        row_iterator = self.bq_validation.collect_row(filtred_df, "ARQUIVO")
        row_iter_content = self.bq_validation.get_row_iterator_content(row_iterator, "ARQUIVO")
        should_process = self.bq_validation.should_process_file(file_name, row_iter_content)
        return should_process


class DataLoad:
    """
    Class is responsible for loading data from a file.
    """
    def __init__(self, logger):
        self.logger = logger
        self.bigquery_save = BigQuerySave(self.logger)

    def run_query(self, dataframe, select_columns):
        """
        Applying a selection to the columns of the dataframe.
        """
        sql_utils = SqlUtils()
        selected_df = sql_utils.run_select(dataframe, select_columns)
        return selected_df

    def save_table_in_bigquery(self, dataframe, target_table, save_mode):
        """
        Saving a table in BigQuery.
        """
        self.bigquery_save.save_table(dataframe, target_table, save_mode)

    def create_external_table(self, bucket_id, blob_path, target_table):
        """ 
        Create external table for the given dataframe.
        """
        query = self.bigquery_save.query_external_table(bucket_id, blob_path, target_table)
        query_job = self.bigquery_save.run_query_job(query)
        self.bigquery_save.job_result(query_job)


class DataProcess:
    """
    Process data from Google Cloud Storage.
    """
    def __init__(self, spark, bucket, bucket_id, local_dir, logger):
        self.spark = spark
        self.bucket = bucket
        self.bucket_id = bucket_id
        self.local_dir = local_dir
        self.logger = logger
        self.processed_files = set()
        self.gcs_handle = GcsHandle(self.bucket, self.logger)
        self.data_load = DataLoad(self.logger)

    def download_file_from_gcs(self, blob, file_name):
        """
        Downloading the file from Google Cloud Storage.
        """
        file_path = self.gcs_handle.download_blob(self.local_dir, blob, file_name)
        return file_path

    def moving_file_in_gcs(self, blob):
        """
        Moving input file from directory "ENTRADA" to directory "BACKUP.
        """
        SOURCE_DIR = "ENTRADA"
        DESTINATION_DIR = "BACKUP"
        self.gcs_handle.move_file(blob, SOURCE_DIR, DESTINATION_DIR)

    def hdfs_process_file(self, file_path, file_name):
        """
        Run the subprocess HDFS.
        """
        self.logger.info("Check if the file exists in HDFS.")
        test_args = ["hadoop", "fs", "-test", "-e", file_name]
        hdfs_check_process = HdfsUtils(test_args)
        file_exists = hdfs_check_process.run_subprocess()

        if file_exists:
            self.logger.info("Removing file from HDFS.")
            rm_args = ["hadoop", "fs", "-rm", file_name]
            hdfs_rm_process = HdfsUtils(rm_args)
            hdfs_rm_process.run_subprocess()

        self.logger.info("Uploading file to HDFS.")
        put_args = ["hadoop", "fs", "-put", file_path]
        hdfs_put_process = HdfsUtils(put_args)
        hdfs_put_process.run_subprocess()

    def read_dataframe_from_file(self, file_name, table_schema):
        """
        Read a PySpark DataFrame from a file.
        """
        dataframe_utils = DataframeUtils(self.spark, file_name, table_schema, self.logger)
        dataframe = dataframe_utils.read_dataframe()
        return dataframe

    def apply_transformations(self, transform_functions, dataframe, file_name):
        """
        Applying functions for data processing.
        """
        transformed_df = transform_functions(dataframe, file_name)
        self.logger.info("Transformations were successfully applied.")
        return transformed_df

    def dataframe_fixing_rows(self, file_name, table_schema, dataframe):
        """
        Fix the rows in table Pescador Artesanal.
        """
        dataframe_utils = DataframeUtils(self.spark, file_name, table_schema, self.logger)
        fixed_df = dataframe_utils.fixing_rows(dataframe)
        return fixed_df

    def process_table_file(self, parameters_for_processing):
        (
            file_name,
            table_schema,
            select_columns,
            transform_functions,
            target_table,
            save_mode,
            staging_list_blobs,
            _,
            _
        ) = parameters_for_processing
        for blob in staging_list_blobs:
            if file_name in blob.name:
                file_path = self.download_file_from_gcs(blob, file_name)
                self.moving_file_in_gcs(blob)
                if os.path.exists(file_path):
                    self.logger.info(f"File {file_name} exists, proceeding with the processing.")
                    self.hdfs_process_file(file_path, file_name)
                    dataframe = self.read_dataframe_from_file(file_name, table_schema)
                    if "PescadorArtesanal.csv" in file_name:
                        dataframe = self.dataframe_fixing_rows(file_name, table_schema, dataframe)
                    dataframe = self.data_load.run_query(dataframe, select_columns)
                    dataframe = self.apply_transformations(transform_functions, dataframe, file_name)
                    self.data_load.save_table_in_bigquery(dataframe, target_table, save_mode)

    def process_blob_parquet(self, blob, trusted_uri, table_schema, select_columns, transform_functions, trusted_blob_path, target_table, save_mode):
        staging_uri = trusted_uri.replace("TRUSTED", "STAGING")
        tag_name = "/".join(staging_uri.split("/")[3:-2])
        if blob.name.startswith(tag_name):
            file_to_process = blob.name.split("/")[-1]
            file_path = self.download_file_from_gcs(blob, file_to_process)
            if os.path.exists(file_path):
                self.logger.info(f"File {file_to_process} exists, proceeding with the processing.")
                self.hdfs_process_file(file_path, file_to_process)
                dataframe = self.read_dataframe_from_file(file_to_process, table_schema)
                dataframe = self.data_load.run_query(dataframe, select_columns)
                dataframe = self.apply_transformations(transform_functions, dataframe, file_to_process)
                self.gcs_handle.write_to_parquet(dataframe, trusted_uri, save_mode)
                # self.data_load.create_external_table(self.bucket_id, trusted_blob_path, target_table)
                self.processed_files.add(file_to_process)

    def process_ext_table_file(self, parameters_for_processing):
        """
        Process the external table file.
        """
        (
            _,
            table_schema,
            select_columns,
            transform_functions,
            target_table,
            save_mode,
            staging_list_blobs,
            trusted_blob_path,
            trusted_uri,
        ) = parameters_for_processing
        for blob in staging_list_blobs:
            self.process_blob_parquet(blob, trusted_uri, table_schema, select_columns, transform_functions, trusted_blob_path, target_table, save_mode)


def main():
    """
    Main function that ingests monthly data from GCS to BigQuery
    """
    stopwatch = Stopwatch()
    stopwatch.start()

    logger = AppLogging()

    # Getting all environment variables for dataproc job
    env_utils = EnvironmentUtils()
    ingestion_args: dict = env_utils.get_args(Variables.INGESTION_ARGS)

    # Setting variables received through arguments on job submit
    PROJECT_ID, BUCKET_ID, DATASET, APP_NAME, MASTER, TMP_DIR, step_ingestion = (
        ingestion_args.get("project_id"),
        ingestion_args.get("bucket_name"),
        ingestion_args.get("dataset"),
        ingestion_args.get("spark_app_name"),
        ingestion_args.get("spark_master"),
        ingestion_args.get("tmp_dir"),
        ingestion_args.get("step_name")
    )
    logger.info(f"{'*' * 50} TASK STARTED {'*' * 50}")

    data_config = DataConfig(PROJECT_ID, BUCKET_ID, DATASET, APP_NAME, MASTER, logger)
    spark = data_config.get_sparksession()
    bucket = data_config.instantiate_bucket()

    data_utils = DataUtils(spark, PROJECT_ID, BUCKET_ID, DATASET, TMP_DIR, logger)
    tasks_for_ingestion = data_utils.get_tasks_for_ingestions(ingestion_args)

    current_date = CurrentDate()
    today = current_date.get_today()
    year, month, day = (today.strftime(fmt) for fmt in ["%Y", "%m", "%d"])

    for task in tasks_for_ingestion.values():
        (
            step_name,
            staging_blob_path,
            snippet_name,
            transform_functions,
            table_schema,
            select_columns,
            table_name,
            save_mode,
            period_to_go,
        ) = data_utils.get_task_details(task)
        if step_name == step_ingestion:
            # Deletes old files that only receive incremental updates.
            data_utils.clearing_bigquery_tables(task)
            for period in range(0, period_to_go):
                # Setting date parameter to process files.
                date_handlers = [DailyDateHandle, MonthlyDateHandle, QuarterDateHandle, YearlyDateHandle]
                day_date, month_date, quarter_date, year_date = [handler(today) for handler in date_handlers]

                # Manipulating target date to handle files with or without date.
                target_date = data_utils.get_target_date(day_date, month_date, quarter_date, year_date, staging_blob_path, period)
                target_date_args = data_utils.get_target_date_args(staging_blob_path, target_date)

                # Setting the target table and file name for processing.
                file_name = data_utils.get_file_name_for_processing(snippet_name, target_date_args)
                target_table = data_utils.get_target_table(table_name)

                # Replace "STAGING" with "TRUSTED" in the blob path to create the trusted blob path
                trusted_blob_path = staging_blob_path.replace("STAGING", "TRUSTED")
                # Get the list of blobs for the staging and trusted blob paths
                staging_list_blobs, trusted_list_blobs = [
                    data_utils.get_list_blobs(bucket, blob_path)
                    for blob_path in [staging_blob_path, trusted_blob_path]
                ]

                staging_uri, trusted_uri = [
                    data_utils.get_uri_gcs(BUCKET_ID, blob_path, year, month, day)
                    for blob_path in [staging_blob_path, trusted_blob_path]
                ]

                # Search for the table type in BigQuery. If it is "EXTERNAL" or "TABLE".
                bq_validation = BigQueryValidation(spark, PROJECT_ID, DATASET, table_name, logger)
                table_type = bq_validation.get_table_type()
                try:
                    # Setting parameters required for file processing.
                    parameters_for_processing = (
                        file_name,
                        table_schema,
                        select_columns,
                        transform_functions,
                        target_table,
                        save_mode,
                        staging_list_blobs,
                        trusted_blob_path,
                        trusted_uri
                    )
                    data_process = DataProcess(spark, bucket, BUCKET_ID, TMP_DIR, logger)
                    data_validate = DataValidate(spark, PROJECT_ID, BUCKET_ID, DATASET, table_name, logger)
                    if table_type == "TABLE":
                        # Validate whether data already exists in the table in BigQuery.
                        should_process_file_in_bq = data_validate.validate_file_in_bq(file_name)
                        if should_process_file_in_bq:
                            data_process.process_table_file(parameters_for_processing)
                    if table_type == "EXTERNAL":
                        # Deletes old files that only receive incremental updates.
                        data_utils.deleting_files_in_gcs(bucket, trusted_list_blobs)
                        # The "tags" are the filter used to process files from the "ANO" and "MES" partition.
                        staging_tag, trusted_tag = [data_utils.get_tag(uri) for uri in [staging_uri, trusted_uri]]
                        # Searching for files that will be validated.
                        staging_blobs, _ = [data_utils.get_list_blobs(bucket, tag) for tag in [staging_tag, trusted_tag]]
                        csv_file_names = [blob.name.split("/")[-1] for blob in staging_blobs]
                        # Validate whether data already exists in the table in Trusted layer.
                        gcs_handle = GcsHandle(bucket, logger)
                        processed_files = data_process.processed_files
                        for csv in csv_file_names:
                            if csv not in processed_files:
                                trusted_path = f"gs://{BUCKET_ID}/{trusted_tag}/*"
                                trusted_df = gcs_handle.get_filtred_dataframe(spark, trusted_path, csv)
                                should_process_file_in_bq = gcs_handle.should_process_file(trusted_df, csv)
                                if should_process_file_in_bq:
                                    data_process.process_ext_table_file(parameters_for_processing)
                except (FileNotFoundError, ValueError, PermissionError, AttributeError) as error:
                    logger.error(error)

    data_utils.delete_temporary_files()

    logger.info("Finished, killing SparkSession.")
    spark.stop()

    logger.info(f"{'*' * 50} TASK FINISHED {'*' * 50}")

    stopwatch.stop()
    stopwatch.elapsed_time(logger)


if __name__ == "__main__":
    main()
