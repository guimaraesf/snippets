# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: sparksession.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code centralizes all methods related to settings for using Spark
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import importlib
import os
import sys

from pyspark.sql import SparkSession

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
Logger = importlib.import_module(LOGGER_MOD_PATH).Logger


class SparkLauncher:
    """
    A class for launching Spark applications.
    """

    def __init__(
        self,
        master: str,
        app_name: str,
        bucket_id: str,
        dataset_id: str,
        project_id: str,
        logger_obj=Logger,
    ) -> None:
        """Initializes the SparkLauncher class with the necessary configurations.

        Args:
            master (str): The master URL of the Spark application.
            app_name (str): The name of the Spark application.
            bucket_id (str): The bucket_id in which the results of the Spark application will be stored.
            dataset_id (str): The dataset that the Spark application will process.
            project_id (str): The ID of the project in which the Spark application is running.
        """
        super().__init__()
        self.master = master
        self.app_name = app_name
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_id = bucket_id
        self.logger = logger_obj()
        self.config_settings = {
            "spark.master": self.master,
            "spark.app.name": self.app_name,
            "temporaryGcsBucket": self.bucket_id,
            "parentProject": self.project_id,
            "viewsEnabled": "true",
            "materializationDataset": self.dataset_id,
        }

    def _set_spark_conf(self, spark: SparkSession) -> None:
        """
        Configures the Spark session with the settings defined in the class.

        Args:
            spark (SparkSession): The Spark session to be configured.
        """
        for propert_name, parameter in self.config_settings.items():
            spark.conf.set(propert_name, parameter)

    def initialize_sparksession(self) -> SparkSession:
        """
        Initializes and configures a SparkSession.

        Returns:
            SparkSession: The initialized and configured Spark session.
        """
        try:
            spark = (
                SparkSession.builder.master(self.master)
                .appName(self.app_name)
                .getOrCreate()
            )
            self.logger.info("The SparkSession was created successfully.")
            self._set_spark_conf(spark)
            return spark
        except Exception as e:
            self.logger.error(e)
            return None
