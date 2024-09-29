#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ================================================================================================
# Module: schema.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This code is responsible for creating the schemas of each table.
# ================================================================================================
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


class Schema:
    """
    This class is responsible for creating the schemas of all tables
    """

    def __init__(self) -> None:
        pass

    def create_schema(self, schema_atrr: dict) -> None:
        """
        Creates a Spark StructType schema based on the provided table information.

        Args:
            schema_atrr (dict): A dictionary where each key is a column name and the value is a tuple containing the data type, nullable flag, and metadata for the column.
        """
        # (e.g., schema_params=(StringType(), True, {"description": ""}))
        schema = StructType()
        for column_name, fields in schema_atrr.items():
            data_type, nullable, metadata = fields.schema_params
            field = StructField(column_name, data_type, nullable, metadata)
            schema.add(field)
