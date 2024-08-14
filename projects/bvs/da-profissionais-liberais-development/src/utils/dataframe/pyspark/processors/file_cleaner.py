# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# ================================================================================================
# Module: cleaner.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for methods that apply treatments to PySpark DataFrames.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import importlib
import os
import sys
from datetime import datetime

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
UTILS_MOD_PATH = get_module_path("src.utils.helpers.utils")
VARIABLES_MOD_PATH = get_module_path("src.utils.helpers.variables")

Logger = importlib.import_module(LOGGER_MOD_PATH).Logger
Utils = importlib.import_module(UTILS_MOD_PATH).Utils
Variables = importlib.import_module(VARIABLES_MOD_PATH).Variables


class CleanTables:
    """
    A class that contains methods for cleaning PySpark DataFrames.
    """

    def __init__(
        self,
        logger_obj=Logger,
        utils=Utils,
        variables=Variables,
    ) -> None:
        """
        Initializes the CleaningTables class.

        Args:
            logger (Logger): The logger to be used.
            utils (Utils): The utility functions to be used.
        """
        self.logger = logger_obj()
        self.utils = utils()
        self.variables = variables
        self.current_date = datetime.now(self.variables.TIMEZONE_BR)

    def _apply_replace_chars(self, df: DataFrame, column: str) -> DataFrame:
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
                        self.variables.CHAR_WITH_ACCENTS,
                        self.variables.CHAR_WITHOUT_ACCENTS,
                    )
                )
            ),
        )
        return df

    def _apply_regexp_replace(
        self, df: DataFrame, column: str, pattern: str, replacement: str
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
        self, df: DataFrame, column: str, replacement_patterns: any
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
        return None

    # def _check_size_key(self, row_key: str) -> None:
    #     """
    #     Checks the size of the row key and logs a warning if it exceeds the recommended size.

    #     Args:
    #         row_key (str): The row key to check.
    #     """
    #     if sys.getsizeof(row_key) > 4000:
    #         self.logger.warning("The row key is above the recommended size of 4KB.")

    # def _create_row_key(self, df: DataFrame, table_infos: dict, delimiter: str) -> DataFrame:
    #     """
    #     Creates a row key for the dataframe based on the table information and a delimiter.

    #     Args:
    #         df (DataFrame): The dataframe for which to create a row key.
    #         table_infos (dict): Information about the table.
    #         delimiter (str): The delimiter to use when concatenating columns to form the row key.

    #     Returns:
    #         DataFrame: The dataframe with the created row key.
    #     """
    #     processing_date = self.current_date.strftime("%Y%m%dT%H%M%S")
    #     list_columns = [
    #         col.columns_to_row_key
    #         for col in table_infos.values()
    #         if col.columns_to_row_key is not None
    #     ][0]
    #     if "cpf_cnpj" in df.columns:
    #         df = df.withColumn("cpf_cnpj", lpad(df["cpf_cnpj"], 14, "0"))
    #     df = df.withColumn("row_key", lit(""))
    #     for column in list_columns:
    #         df = df.withColumn("row_key", concat(df["row_key"], df[column], lit(delimiter)))
    #     df = df.withColumn("row_key", concat(df["row_key"], lit(processing_date)))
    #     df = self._apply_regexp_replace(df, "row_key", r" ", r"")
    #     row_key_element = df.select("row_key").first()[0]
    #     self._check_size_key(row_key_element)
    #     return df

    def _adding_control_column(self, df: DataFrame, file_name: str) -> DataFrame:
        """
        Adding control columns in DataFrame.

        Args:
            df (DataFrame): The DataFrame to be processed.
            file_name (str): The file name to be processed.

        Returns:
            DataFrame: The processed DataFrame.
        """
        processing_date = self.current_date.strftime("%Y-%m-%d %H:%M:%S")
        df = df.withColumn("arquivo", lit(file_name))
        df = df.withColumn("data_processamento", lit(processing_date))
        return df

    def process_df(self, df: DataFrame, table_infos: dict, file_name: str) -> DataFrame:
        """
        Processes a DataFrame based on provided column information.

        Args:
            df (DataFrame): The DataFrame to be processed.
            table_infos (dict): Information about the columns to be processed.
            file_name (str): The file name to be  processed.

        Returns:
            DataFrame: The processed DataFrame.
        """
        # This position "infos.regex" contains the replacement patterns to be applied.
        # (e.g., regexp_replace(r"[^\w\s]", r""))
        for column_name, infos in table_infos.items():
            df = self._apply_all_transformations(df, column_name, infos.regex)
        df = self._adding_control_column(df, file_name)
        return df
