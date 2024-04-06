# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: columns.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code centralizes all columns to use in spark schemas.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from typing import NamedTuple
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType


class DataframeParams(NamedTuple):
    """
    A NamedTuple for holding parameters related to a DataFrame.

    Args:
        schema_params (tuple): A tuple containing parameters related (data type, nullable flag, and metadata) to the schema of the DataFrame. 
        regex (tuple): A tuple containing regular expression for data processing.
    """
    schema_params: tuple
    regex: tuple

class Columns(enumerate):
    """
    Class to set all columns for schema on DataFrames.
    """

    string_type = StringType()
    timestamp_type = TimestampType()
    float_type = FloatType()
    integer_type = IntegerType()
    STRIP_NON_ALPHANUMERIC = r"[^\w\s]"
    EMPTY = r""

    TABLE_INFOS_NAME: dict = {
        "name_column": DataframeParams(
            schema_params=(string_type, True, {"description": ""}),
            regex=((STRIP_NON_ALPHANUMERIC, EMPTY)),
        )
    }