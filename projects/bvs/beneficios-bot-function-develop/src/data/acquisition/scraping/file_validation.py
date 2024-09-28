#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: file_validation.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code ...
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import zipfile
import csv
from abc import ABC, abstractmethod


class Validator(ABC):
    """
    Abstract class for validators.
    """

    @abstractmethod
    def validate(self):
        """
        Validates the data.
        """


class FileValidator(Validator):
    """
    Validator for file existence.
    """

    def __init__(self, file_path, logger):
        self.file_path = file_path
        self.logger = logger

    def validate(self):
        """
        Checks if the file exists.
        :return: True if the file exists, False otherwise.
        """
        try:
            if os.path.exists(self.file_path):
                return True

        except FileNotFoundError as exception:
            self.logger.error(f"File not found: {exception}")


class ZipfileValidator(Validator):
    """
    Validator for zipfile validity.
    """

    def __init__(self, zipfile_path, logger):
        self.zipfile_path = zipfile_path
        self.logger = logger

    def validate(self):
        """
        Checks if the zipfile is valid.
        :return: True if the zipfile is valid, False otherwise.
        """
        try:
            if zipfile.is_zipfile(self.zipfile_path):
                return True

        except zipfile.BadZipFile as execption:
            self.logger.error(f"Failed to extract zip file: {execption}")


# class CsvValidator:
#     def __init__(self, logger):
#         self.logger = logger

#     def validate_num_columns(self, file_path, encoding):
#         self.logger.info("Validating whether the file presents any inconsistencies.")
#         with open(file_path, 'r', encoding=encoding) as csv_file:
#             reader = csv.reader(csv_file)
#             num_cols = None
#             for row in reader:
#                 if num_cols is None:
#                     num_cols = len(row)
#                 elif len(row) != num_cols:
#                     return False
#         return True

# def check_empty_lines(filename):
#     with open(filename, 'r') as f:
#         reader = csv.reader(f)
#         lines = list(reader)
#         empty_lines = [i for i, line in enumerate(lines) if not line]
#     return empty_lines

# def check_delimiter_consistency(filename, delimiter):
#     with open(filename, 'r') as f:
#         content = f.read()
#     lines = content.split('\n')
#     inconsistencies = [i for i, line in enumerate(lines) if line.count(delimiter) != lines[0].count(delimiter)]
#     return inconsistencies
