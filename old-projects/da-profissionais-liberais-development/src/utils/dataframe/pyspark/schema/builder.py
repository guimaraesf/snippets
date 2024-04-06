#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ================================================================================================
# Module: builder.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code is responsible for creating the schemas of each table.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from pyspark.sql.types import DataType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


class SchemaTables:
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

    @staticmethod
    def _get_fields(infos: tuple) -> tuple[DataType, bool, dict]:
        """
        Extracts the fields from the provided information.

        Args:
            infos (tuple): The information from which to extract the fields.

        Returns:
            tuple[DataType, bool, dict]: A tuple containing the DataType, a boolean value, and a dictionary.
        """
        schema_params = infos[0]
        if isinstance(schema_params, tuple):
            return schema_params
        return infos

    def create_spark_struct_type(self, table_infos: dict) -> None:
        """
        Creates a Spark StructType schema based on the provided table information.

        Args:
            table_infos (dict): A dictionary where each key is a column name and the value is a tuple containing the data type, nullable flag, and metadata for the column.
        """
        for column_name, infos in table_infos.items():
            data_type, nullable, metadata = self._get_fields(infos)
            field = StructField(column_name, data_type, nullable, metadata)
            self.get_schema.add(field)
