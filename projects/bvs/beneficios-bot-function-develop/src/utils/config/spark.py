#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: spark.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code centralizes all methods related to settings for using Spark
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================

import os
import sys
from pyspark.sql import SparkSession

# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))


class SparkLauncher:
    """
    SparkLauncher class
    """

    def __init__(self, master, app_name, project_id, dataset, bucket):
        self.master = master
        self.app_name = app_name
        self.project_id = project_id
        self.dataset = dataset
        self.bucket = bucket
        self.config_settings = {
            "spark.master": self.master,
            "spark.app.name": self.app_name,
            "temporaryGcsBucket": self.bucket,
            "parentProject": self.project_id,
            "viewsEnabled": "true",
            "materializationDataset": self.dataset,
            "spark.sql.debug.maxToStringFields": 100,
        }

    # def __create_logger(self, spark_context):
    #     """
    #     Create a logger
    #     """

    #     log4j_logger = spark_context._jvm.org.apache.log4j
    #     logger = log4j_logger.LogManager.getLogger(__name__)

    #     # Customize log format
    #     log_pattern = "%d{yyyy-MM-dd HH:mm:ss} - %p - %c - %m%n"
    #     log_layout = log4j_logger.PatternLayout(log_pattern)

    #     # Create a new ConsoleAppender to save logs to a file
    #     console_appender = log4j_logger.ConsoleAppender(log_layout)
    #     logger.addAppender(console_appender)

    #     # Set log level to INFO
    #     logger.setLevel(log4j_logger.Level.INFO)

    #     # Get the root logger and set the log level to ERROR
    #     root_logger = log4j_logger.LogManager.getRootLogger()
    #     root_logger.setLevel(log4j_logger.Level.INFO)
    #     return logger

    def __configure_session(self, spark):
        """
        Configure SparkSession
        """
        for propert_name, parameter in self.config_settings.items():
            spark.conf.set(propert_name, parameter)

    def initialize_sparksession(self):
        """
        Initialize SparkSession
        """
        spark = (
            SparkSession.builder.master(self.master).appName(self.app_name)  # .config(
            #     "spark.driver.extraJavaOptions",
            #     "-Dlog4j.configuration=file:log4j.properties"
            # ) \
            .getOrCreate()
        )

        self.__configure_session(spark)

        return spark
