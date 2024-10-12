# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# ================================================================================================
# Module: transform.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This module is responsible for methods that apply treatments to PySpark DataFrames.
# ================================================================================================
from datetime import datetime
from pyspark.sql import DataFrame
import pyspark.sql.functions as fn
import pytz


class Transform:
    """
    A class that contains methods for cleaning PySpark DataFrames.
    """

    def __init__(self) -> None:
        """
        Initializes the Transform class.
        """
        self.current_date = datetime.now(pytz.timezone("America/Sao_Paulo"))

    def normalize_columns(df: DataFrame) -> DataFrame:
        """
        Replaces characters in a DataFrame column.

        Args:
            df (DataFrame): The DataFrame to be processed.

        Returns:
            DataFrame: The processed DataFrame.
        """
        CHARS_WITH_ACCENTS = "áéíóúÁÉÍÓÚâêîôûÂÊÎÔÛàèìòùÀÈÌÒÙãõÃÕçÇäëïöüÄËÏÖÜ"
        CHARS_WITHOUT_ACCENTS = "aeiouAEIOUaeiouAEIOUaeiouAEIOUaoAOcCaeiouAEIOU"
        for column in df.columns:
            df = df.withColumn(
                column,
                fn.trim(
                    fn.upper(
                        fn.translate(
                            column,
                            CHARS_WITH_ACCENTS,
                            CHARS_WITHOUT_ACCENTS,
                        )
                    )
                ),
            )
        return df
