#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: cleaning.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module centralizes all lists with table column names
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
from datetime import datetime
import pytz
from pyspark.sql.functions import (
    translate,
    regexp_replace,
    col,
    lit,
    concat,
    substring,
    upper,
    trim,
)

# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))


class Cleaning:
    """
    A class that contains methods for cleaning PySpark DataFrames.
    """

    def __init__(self):
        # Setting processing date parameters
        self.characters_with_accents = "áéíóúÁÉÍÓÚâêîôûÂÊÎÔÛàèìòùÀÈÌÒÙãõÃÕçÇäëïöüÄËÏÖÜ"
        self.characters_without_accents = (
            "aeiouAEIOUaeiouAEIOUaeiouAEIOUaoAOcCaeiouAEIOU"
        )
        self.time_zone = pytz.timezone("America/Sao_Paulo")
        self.processing_date = datetime.now(self.time_zone).strftime(
            "%d-%m-%Y %H:%M:%S"
        )

    def __replace_accents(self, dataframe, column):
        """
        Replaces characters with accents in a DataFrame column.
        """
        dataframe = dataframe.withColumn(
            column,
            trim(
                upper(
                    translate(
                        column,
                        self.characters_with_accents,
                        self.characters_without_accents,
                    )
                )
            ),
        )
        return dataframe

    @staticmethod
    def __apply_regex_transformations(dataframe, column, pattern, replacement):
        """
        Applies a regex transformation to a DataFrame column.
        """
        dataframe = dataframe.withColumn(
            column, regexp_replace(col(column), pattern, replacement)
        )
        return dataframe

    @staticmethod
    def __add_control_columns(dataframe, file_name, processing_date):
        """
        Adds control columns to a DataFrame.
        """
        columns_to_add = {
            "ARQUIVO": lit(file_name),
            "DATA_PROCESSAMENTO": lit(processing_date),
        }
        for column_name, column_value in columns_to_add.items():
            dataframe = dataframe.withColumn(column_name, column_value)
        return dataframe

    def process_dataframe(self, dataframe, file_name, dict_regex, processing_date):
        """
        Processes a DataFrame by applying specific transformations to certain columns and adding control columns.

        Args:
            dataframe (pyspark.sql.DataFrame): The input DataFrame to be processed.
            dict_regex (dict): A dictionary containing the columns and their respective transformations.
            processing_date (str): The target date for processing.
            logger (Logger): The logger object used for logging.

        Returns:
            dataframe (pyspark.sql.DataFrame): The processed DataFrame with the applied transformations and control columns.
        """

        for column, transformations in dict_regex.items():
            dataframe = self.__replace_accents(dataframe, column)
            for pattern, replacement in transformations:
                dataframe = self.__apply_regex_transformations(
                    dataframe, column, pattern, replacement
                )
        dataframe = self.__add_control_columns(dataframe, file_name, processing_date)
        return dataframe


class Formatting:
    """
    A class that contains methods for formatting dates in PySpark DataFrames.
    """

    def __init__(self):
        pass

    @staticmethod
    def __format_complete_date(
        dataframe, column, start1, len1, start2, len2, start3, len3
    ):
        """
        Formats a complete date column in a DataFrame by concatenating substrings of the column values.
        """
        dataframe = dataframe.withColumn(
            column,
            concat(
                substring(col(column), start1, len1),
                lit("-"),
                substring(col(column), start2, len2),
                lit("-"),
                substring(col(column), start3, len3),
            ),
        )
        return dataframe

    @staticmethod
    def __format_partial_date(dataframe, column, start1, len1, start2, len2):
        """
        Formats a partial date column in a DataFrame by concatenating substrings of the column values.
        """
        dataframe = dataframe.withColumn(
            column,
            concat(
                substring(col(column), start1, len1),
                lit("-"),
                substring(col(column), start2, len2),
                lit("-01"),
            ),
        )
        return dataframe

    def format_date_columns(
        self, dataframe, columns, start1, len1, start2, len2, start3=None, len3=None
    ):
        """
        Formats date columns in a DataFrame.
        """
        for column in columns:
            if start3 is not None and len3 is not None:
                dataframe = self.__format_complete_date(
                    dataframe, column, start1, len1, start2, len2, start3, len3
                )
            else:
                dataframe = self.__format_partial_date(
                    dataframe, column, start1, len1, start2, len2
                )
        return dataframe
