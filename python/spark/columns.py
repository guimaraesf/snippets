# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: columns.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This code centralizes all columns to use in spark schemas.
# ================================================================================================
import pyspark
from typing import NamedTuple
from enum import Enum


class DataframeSchema(NamedTuple):
    """
    A NamedTuple for holding parameters related to a DataFrame.

    Args:
        schema_params (tuple): A tuple containing parameters related (data type, nullable flag, and metadata) to the schema of the DataFrame.
        regex (tuple): A tuple containing regular expression for data processing.
    """

    # NOTE: (e.g., StructType(dataType, nullable, metadata))
    schema_attr: tuple[pyspark.sql.types, bool, dict]
    regex: tuple[str, str]


class Columns(Enum):
    """
    Class to set all columns for schema on DataFrames.
    """

    @classmethod
    def init(cls):
        return {var.name: var.value for var in cls}

    PATTERN = ""
    REPLACEMENT = ""

    TABLE_NAME: dict = {
        "column_name": DataframeSchema(
            schema_attr=("dataType", "nullable", {"description": ""}),
            regex=((PATTERN, REPLACEMENT)),
        )
    }
