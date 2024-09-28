# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# ================================================================================================
# Module: handler.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for methods that apply operations to PySpark DataFrames.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import importlib
import os
import sys
from typing import Optional

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.storagelevel import StorageLevel

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


LOGGER_MOD_PATH = get_module_path("src.utils.helpers.logger")
Logger = importlib.import_module(LOGGER_MOD_PATH).Logger


class PysparkHandler:
    """
    Handles operations related to PySpark.
    """

    def __init__(self, logger_obj=Logger) -> None:
        """
        Initializes the PysparkHandler with a logger.

        Args:
            logger (Logger, optional): The logger to be used. Defaults to Logger.
        """
        self.logger = logger_obj()

    def read_parquet_file(self, spark: SparkSession, file_path: str) -> DataFrame:
        """
        Reads a PARQUET file into a DataFrame using PySpark.

        Args:
            spark (SparkSession): The SparkSession to use.
            file_path (str): The path to the CSV file.

        Returns:
            DataFrame: The DataFrame created from the CSV file.
        """
        try:
            df = spark.read.parquet(file_path)
            return df
        except Py4JJavaError as e:
            self.logger.error(e)
            return None

    def read_csv_file(
        self,
        spark: SparkSession,
        file_path: str,
        delimiter: str,
        header: bool,
        encoding: str,
        schema: StructType,
    ) -> DataFrame:
        """
        Reads a CSV file into a DataFrame using PySpark.

        Args:
            spark (SparkSession): The SparkSession to use.
            file_path (str): The path to the CSV file.
            delimiter (str): The delimiter used in the CSV file.
            header (bool): Whether the CSV file has a header.
            encoding (str): The encoding of the CSV file.
            schema (StructType): The schema to use when reading the CSV file.

        Returns:
            DataFrame: The DataFrame created from the CSV file.
        """
        try:
            df = (
                spark.read.option("delimiter", delimiter)
                .option("encoding", encoding)
                .option("recursiveFileLookup", "true")
                .csv(file_path, header=header, schema=schema)
            )
            return df
        except Py4JJavaError as e:
            self.logger.error(e)
            return None

    def write_file(
        self,
        df: DataFrame,
        write_format: str,
        dest_path: str,
        mode: str,
        delimiter: Optional[str] = None,
        header: Optional[bool] = None,
        encoding: Optional[str] = None,
    ) -> None:
        """
        Writes a DataFrame to a file in the specified format.

        Args:
            df (DataFrame): The DataFrame to write.
            write_format (str): The format to write the DataFrame in.
            dest_path (str): The destination path for the file.
            mode (str): The write mode (e.g., 'overwrite', 'append').
            delimiter (str, optional): The delimiter to use (if applicable). Defaults to None.
            header (bool, optional): Whether to write a header (if applicable). Defaults to None.
            encoding (str, optional): The encoding to use (if applicable). Defaults to None.
        """
        try:
            PARTITIONS = 10
            # Persist the DataFrame in memory and disk
            df.persist(StorageLevel.MEMORY_AND_DISK)
            if write_format == "csv":
                df.coalesce(PARTITIONS).write.mode(mode).options(
                    delimiter=delimiter,
                    encoding=encoding,
                    header=header,
                    maxRecordsPerFile=100000,
                ).csv(dest_path)
            if write_format == "parquet":
                df.coalesce(PARTITIONS).write.mode(mode).options(
                    maxRecordsPerFile=100000
                ).parquet(dest_path)
        except Py4JJavaError as e:
            self.logger.error(e)
        finally:
            # Unpersist the DataFrame to free up memory
            df.unpersist()
