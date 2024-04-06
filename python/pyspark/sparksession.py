# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: sparksession.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This code centralizes all methods related to settings for using Spark
# ================================================================================================
from pyspark.sql import SparkSession


class SparkSessionInit:
    """
    A class for initialize Spark applications.
    """

    def __init__(
        self,
        spark_conf: dict,
    ) -> None:
        """
        Initializes the SparkSessionInit class.

        Args:
            spark_conf (dict): A dictionary containing the configuration settings for the SparkSession.
        """
        self.spark_conf = spark_conf

    def _set_spark_conf(self, spark: SparkSession) -> None:
        """
        Configures the Spark session with the settings defined in the class.

        Args:
            spark (SparkSession): The Spark session to be configured.
        """
        for key, value in self.spark_conf.items():
            spark.conf.set(key, value)

    def initialize_sparksession(self) -> SparkSession:
        """
        Initializes and configures a SparkSession.

        Returns:
            SparkSession: The initialized and configured Spark session.
        """
        MASTER, APP_NAME = (self.spark_conf["master"], self.spark_conf["app_name"])
        spark = SparkSession.builder.master(MASTER).appName(APP_NAME).getOrCreate()
        self._set_spark_conf(spark)
        return spark
