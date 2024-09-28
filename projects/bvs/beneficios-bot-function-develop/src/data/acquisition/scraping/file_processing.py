#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: file_processing.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code ...
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import csv
import pandas as pd
from pandas.errors import ParserError


class PandasCsvHandle:
    """
    This class provides methods for reading data from a Pandas.
    """

    def __init__(self, logger) -> None:
        self.logger = logger

    def read_csv_file(
        self, file_path, sep, encoding, header=None, engine=None, on_bad_lines="skip"
    ):
        """
        Reads a CSV file from the given file path and returns a DataFrame.
        """
        try:
            dataframe = pd.read_csv(
                file_path,
                sep=sep,
                encoding=encoding,
                header=header,
                engine=engine,
                on_bad_lines=on_bad_lines,
            )
            return dataframe
        except FileNotFoundError as error:
            self.logger.error(f"FileNotFoundError: {error}")
        except ParserError as error:
            self.logger.error(f"OnBadLines in file: {error}")

    def convert_to_csv_file(self, dataframe, file_path, sep, encoding, index=False):
        """
        Converts a DataFrame to a CSV file and saves it to the given file path.
        """
        dataframe.to_csv(file_path, sep=sep, encoding=encoding, index=index)

    def concat_dataframes(self, dataframes):
        """
        Concatenates a list of DataFrames into a single DataFrame.
        """
        dataframe = pd.concat(dataframes)
        return dataframe
