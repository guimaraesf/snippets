# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# ================================================================================================
# Module: writers.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This module is responsible for methods that apply operations to PySpark DataFrames.
# ================================================================================================
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel


class PysparkWriters:
    """
    Writers operations related to PySpark.
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def write_csv_file(
        df: DataFrame,
        dest_path: str,
        mode: str,
        delimiter: Optional[str] = None,
        header: Optional[bool] = None,
        encoding: Optional[str] = None,
    ) -> None:
        PARTITIONS, MAX_RECORDS_PER_FILE = (10, 100000)
        # Persist the DataFrame in memory and disk
        df.persist(StorageLevel.MEMORY_AND_DISK)
        if dest_path.endswith(".csv"):
            df.coalesce(PARTITIONS).write.mode(mode).options(
                delimiter=delimiter,
                encoding=encoding,
                header=header,
                maxRecordsPerFile=MAX_RECORDS_PER_FILE,
            ).csv(dest_path)
        # Unpersist the DataFrame to free up memory
        df.unpersist()

    @staticmethod
    def write_parquet_file(
        df: DataFrame,
        dest_path: str,
        mode: str,
    ) -> None:
        PARTITIONS, MAX_RECORDS_PER_FILE = (10, 100000)
        # Persist the DataFrame in memory and disk
        df.persist(StorageLevel.MEMORY_AND_DISK)
        if dest_path.endswith(".parquet"):
            df.coalesce(PARTITIONS).write.mode(mode).options(
                maxRecordsPerFile=MAX_RECORDS_PER_FILE
            ).parquet(dest_path)
        # Unpersist the DataFrame to free up memory
        df.unpersist()
