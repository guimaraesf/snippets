# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# ================================================================================================
# Module: readers.py
# Author: Fernando Theodoro Guimarães
# E-mail: 
# Description: This module is responsible for methods that apply operations to PySpark DataFrames.
# ================================================================================================
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class PysparkReaders:
    """
    Handles operations related to PySpark.
    """

    def __init__(self) -> None:
        """
        Initializes the PysparkHandler with a logger.
        """

    def read_parquet_file(self, spark: SparkSession, file_path: str) -> DataFrame | None:
        """
        Reads a PARQUET file into a DataFrame using PySpark.

        Args:
            spark (SparkSession): The SparkSession to use.
            file_path (str): The path to the CSV file.

        Returns:
            DataFrame: The DataFrame created from the CSV file.
        """
        if file_path.endswith(".parquet"):
            df = spark.read.parquet(file_path)
            return df
        return None

    def read_csv_file(
        self,
        spark: SparkSession,
        file_path: str,
        delimiter: str,
        header: bool,
        encoding: str,
        schema: StructType,
    ) -> DataFrame | None:
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
        if file_path.endswith(".csv"):
            df = (
                spark.read.option("delimiter", delimiter)
                .option("encoding", encoding)
                .option("recursiveFileLookup", "true")
                .csv(file_path, header=header, schema=schema)
            )
            return df
        return None
