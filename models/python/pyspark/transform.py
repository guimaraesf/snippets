# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# ================================================================================================
# Module: transform.py
# Author: Fernando Theodoro Guimarães
# E-mail: 
# Description: This module is responsible for methods that apply treatments to PySpark DataFrames.
# ================================================================================================
from datetime import datetime
import pytz

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
from pyspark.sql.functions import lpad
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import translate
from pyspark.sql.functions import trim
from pyspark.sql.functions import upper


class Transform:
    """
    A class that contains methods for cleaning PySpark DataFrames.
    """

    def __init__(
        self
    ) -> None:
        """
        Initializes the Transform class.
        """
        self.current_date = datetime.now(pytz.timezone("America/Sao_Paulo"))

    @staticmethod
    def _apply_replace_chars(df: DataFrame, column: str) -> DataFrame:
        """
        Replaces characters in a DataFrame column.

        Args:
            df (DataFrame): The DataFrame to be processed.
            column (str): The column in which characters are to be replaced.

        Returns:
            DataFrame: The processed DataFrame.
        """
        df = df.withColumn(
            column,
            trim(
                upper(
                    translate(
                        column,
                        "áéíóúÁÉÍÓÚâêîôûÂÊÎÔÛàèìòùÀÈÌÒÙãõÃÕçÇäëïöüÄËÏÖÜ",
                        "aeiouAEIOUaeiouAEIOUaeiouAEIOUaoAOcCaeiouAEIOU",
                    )
                )
            ),
        )
        return df

    @staticmethod
    def _apply_regexp_replace(
        df: DataFrame, column: str, pattern: str, replacement: str
    ) -> DataFrame:
        """
        Applies a regular expression replacement to a DataFrame column.

        Args:
            df (DataFrame): The DataFrame to be processed.
            column (str): The column in which the replacement is to be applied.
            pattern (str): The pattern to be replaced.
            replacement (str): The string to replace the pattern with.

        Returns:
            DataFrame: The processed DataFrame.
        """
        df = df.withColumn(column, regexp_replace(col(column), pattern, replacement))
        return df

    def _apply_all_transformations(
        self, df: DataFrame, column: str, replacement_patterns
    ) -> DataFrame:
        """
        Applies all transformations to a DataFrame column.

        Args:
            df (DataFrame): The DataFrame to be processed.
            column (str): The column to be transformed.
            replacement_patterns (any): The replacement patterns to be applied.

        Returns:
            DataFrame: The processed DataFrame.
        """
        first_element = replacement_patterns[0]

        df = self._apply_replace_chars(df, column)
        if isinstance(first_element, str):
            # If the first term is a string, I know there is only one pattern to apply.
            pattern, replacement = replacement_patterns
            df = self._apply_regexp_replace(df, column, pattern, replacement)
            return df
        if isinstance(first_element, tuple):
            # If the first term is a tuple, I know that there are more than 1 regex pattern to be applied.
            for pattern, replacement in replacement_patterns:
                df = self._apply_regexp_replace(df, column, pattern, replacement)
                return df
        return df

    @staticmethod
    def _add_custom_columns(df: DataFrame) -> DataFrame:
        """
        add control columns in DataFrame.

        Args:
            df (DataFrame): The DataFrame to be processed.

        Returns:
            DataFrame: The processed DataFrame.
        """
        df = df.withColumn("", lit(""))
        return df

    def process_df(self, df: DataFrame, table_infos: dict) -> DataFrame:
        """
        Processes a DataFrame based on provided column information.

        Args:
            df (DataFrame): The DataFrame to be processed.
            table_infos (dict): Information about the columns to be processed.

        Returns:
            DataFrame: The processed DataFrame.
        """
        # This position "fields.regex" contains the replacement patterns to be applied.
        # (e.g., regexp_replace=(r"[^\w\s]", r""))
        for column_name, fields in table_infos.items():
            df = self._apply_all_transformations(df, column_name, fields.regex)
        return df
