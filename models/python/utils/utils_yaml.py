#-*- coding: utf-8 -*-
#!/usr/bin/env python3

# ================================================================================================
# Module: *.py
# Author: Fernando Theodoro Guimarães
# Description:
# ================================================================================================
import yaml


class UtilsYaml:
    """
    """
    def __init__(self):
        pass

    @staticmethod
    def open_yaml_file(file_path: str) -> dict:
        """
        Opens the YAML file and returns its contents as a dictionary.
    
        Parameters
        ----------
        file_path : str
            The path to the YAML file to be handled.
    
        Returns
        -------
        dict
            The contents of the YAML file as a dictionary.
        """
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
        return data
