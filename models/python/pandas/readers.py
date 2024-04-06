# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: readers.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This module is responsible for methods that apply operations to Pandas DataFrames.
# ================================================================================================
from typing import Optional
import pandas as pd


class Readers:
    """
    This class provides methods for reading data from a Pandas.
    """

    def __init__(self) -> None:
        """
        Initializes the PandasCsvHandle object..
        """
        pass

    @staticmethod
    def create_df(data: str, columns: list, index: Optional[any] = None) -> pd.DataFrame:
        """
        Creates a pandas DataFrame from file.

        Args:
            data (str): The path to the data.
            columns (list): The list with name columns of file.
            index (any, optional): Whether to write row names. Defaults to None.
        Returns:
            pd.DataFrame: The DataFrame obtained from the JSON file.
        """
        df = pd.DataFrame(data=data, index=index, columns=columns)
        return df

    @staticmethod
    def normalize_json_file(file: str) -> pd.DataFrame:
        """
        Normalize a JSON file and converts it into a pandas DataFrame.

        Args:
            file (str): The path to the JSON file.

        Returns:
            pd.DataFrame: The DataFrame obtained from the JSON file.
        """

        df = pd.json_normalize(file)
        return df

    @staticmethod
    def read_json_file(
        file_path: str,
        encoding: str,
        orient: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Reads a JSON file and converts it into a pandas DataFrame.

        Args:
            file_path (str): The path to the JSON file.
            encoding (str): The encoding used in the JSON file.
            orient (str): Indication of expected JSON string format.

        Returns:
            pd.DataFrame: The DataFrame obtained from the JSON file.
        """
        df = pd.read_json(file_path, encoding=encoding, orient=orient)
        return df

    @staticmethod
    def read_csv_file(
        file_path: str,
        sep: str,
        encoding: str,
        header: Optional[bool] = None,
        engine: Optional[str] = None,
        on_bad_lines: str = "skip",
    ) -> pd.DataFrame:
        """
        Reads a CSV file and converts it into a pandas DataFrame.

        Args:
            file_path (str): The path to the CSV file.
            sep (str): The separator used in the CSV file.
            encoding (str): The encoding used in the CSV file.
            header (bool, optional): Whether to use the first row as header. Defaults to None.
            engine (str, optional): The engine to use for reading the CSV file. Defaults to None.
            on_bad_lines (str, optional): What to do when encountering bad lines. Defaults to "skip".

        Returns:
            pd.DataFrame: The DataFrame obtained from the CSV file.
        """
        df = pd.read_csv(
            file_path,
            sep=sep,
            encoding=encoding,
            header=header,
            engine=engine,
            on_bad_lines=on_bad_lines,
        )
        return df

    @staticmethod
    def convert_to_csv_file(
        df: pd.DataFrame,
        file_path: Optional[str] = None,
        sep: str = ";",
        encoding: Optional[str] = None,
        mode: str = "w",
        header: Optional[bool] = True,
        index: Optional[bool] = False,
    ) -> str:
        """
        Converts a pandas DataFrame into a CSV file.

        Args:
            df (pd.DataFrame): The DataFrame to be converted.
            file_path (str, optional): The path where the CSV file will be saved. Defaults to None.
            sep (str, optional): The separator to be used in the CSV file. Defaults to ";".
            encoding (str, optional): The encoding to be used in the CSV file. Defaults to None.
            mode (str, optional): The mode in which the file is opened. Defaults to "w".
            header (bool, optional): Whether to write out the column names. Defaults to True.
            index (bool, optional): Whether to write row names. Defaults to False.
        """
        df = df.to_csv(
            path_or_buf=file_path,
            sep=sep,
            encoding=encoding,
            mode=mode,
            header=header,
            index=index
        )
        return df

    @staticmethod
    def convert_to_parquet_file(
        df: pd.DataFrame,
        file_path: str,
        compression: str = "snappy",
        index: bool = False,
    ) -> None:
        """
        Converts a pandas DataFrame into a Parquet file.

        Args:
            df (pd.DataFrame): The DataFrame to be converted.
            file_path (str): The path where the Parquet file will be saved.
            compression (str, optional): The compression method used in the Parquet file. Defaults to "snappy".
            index (bool, optional): Whether to write row names. Defaults to False.
        """
        df.to_parquet(path=file_path, compression=compression, index=index)

    @staticmethod
    def concat_dataframes(dfs: list) -> pd.DataFrame:
        """
        Concatenates a list of pandas DataFrames into one DataFrame.

        Args:
            dfs (list): A list of DataFrames to be concatenated.

        Returns:
            pd.DataFrame: The concatenated DataFrame.
        """
        df = pd.concat(dfs)
        return df
