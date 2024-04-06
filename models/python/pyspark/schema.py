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


class SchemaBuilder:
    """
    This class is responsible for creating the schemas of all tables
    """

    def __init__(self) -> None:
        self.schema = StructType()

    @property
    def get_schema(self) -> StructType:
        """
        Gets the schema of the instance.

        Returns:
            StructType: The schema of the instance.
        """
        return self.schema

    def create_spark_struct_type(self, schema_infos: dict) -> None:
        """
        Creates a Spark StructType schema based on the provided table information.

        Args:
            schema_infos (dict): A dictionary where each key is a column name and the value is a tuple containing the data type, nullable flag, and metadata for the column.
        """
        # This position "fields.schema_params" contains the replacement patterns to be applied.
        # (e.g., schema_params=(StringType(), True, {"description": ""}))
        for column_name, fields in schema_infos.items():
            data_type, nullable, metadata = fields.schema_params
            field = StructField(column_name, data_type, nullable, metadata)
            self.get_schema.add(field)
