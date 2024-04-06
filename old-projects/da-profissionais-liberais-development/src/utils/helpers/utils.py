# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: utils.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code centralizes all methods to use in other modules
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import importlib
import os
import sys

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


class Utils:
    """
    A utility class that provides various helper methods.
    """

    def __init__(self, logger_obj=Logger) -> None:
        """
        Initializes the Utils class.

        Args:
            logger (logging.Logger): The logger to be used for logging.
        """
        self.logger = logger_obj()

    def check_variables(self, class_variables: callable) -> None:
        """
        Check if all variables defined in the Variables class have been initialized.

        Args:
            class_variables (callable): Instance of class Variables.
        """
        for key, value in class_variables.get_all_variables():
            if not value:
                self.logger.error(
                    f"The variable {key} was not created, because the value is: {value}"
                )


class UtilsDataproc:
    """
    It contains several properties that return various attributes.
    """

    def __init__(self) -> None:
        """
        Initializes the UtilsDataproc class.
        """

    @property
    def get_step_id(self):
        """Returns the step id."""
        return sys.argv[1]

    @property
    def get_bucket_id(self):
        """Returns the ID of the bucket."""
        return sys.argv[2]

    @property
    def get_cluster_id(self):
        """Returns the ID of the dataproc cluster."""
        return sys.argv[3]

    @property
    def get_dataset_id(self):
        """Returns the ID of the dataset."""
        return sys.argv[4]

    @property
    def get_project_id(self):
        """Returns the ID of the project."""
        return sys.argv[5]

    @property
    def get_spark_app_name(self):
        """Returns the spark app name."""
        return sys.argv[6]

    @property
    def get_spark_master(self):
        """Returns the spark url master."""
        return sys.argv[7]

    @property
    def get_port(self):
        """Returns the number port."""
        return sys.argv[8]

    @property
    def get_tmp_dir(self):
        """Returns the tmp dir."""
        return sys.argv[9]
