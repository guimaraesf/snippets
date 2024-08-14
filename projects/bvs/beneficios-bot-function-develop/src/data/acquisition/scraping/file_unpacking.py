#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: file_unpacking.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code ...
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import zipfile
from zipfile import BadZipfile


class FileUnpacker:
    """
    Class for unpacking files.
    """

    def __init__(self, source_path, extract_path, logger):
        self.source_path = source_path
        self.extract_path = extract_path
        self.logger = logger
        self.extensions = [".csv", ".xlsx", ".xls"]

    def extract_directories(self, zip_ref):
        """
        Extract directories from zipfile.
        """
        # The value 2064 for "directories"
        # is equivalent to the value 0x810 in hexadecimal.
        for file_info in zip_ref.infolist():
            file_attribute = file_info.external_attr
            if file_attribute == 2064:
                zip_ref.extractall(self.extract_path)
                break

    def extract_files(self, zip_ref):
        """
        Extract file from zipfile.
        """
        # The value 2080 for "reguler files"
        # is equivalent to the value 0x820 in hexadecimal.
        for file_info in zip_ref.infolist():
            file_name = file_info.filename
            if any(file_name.endswith(ext) for ext in self.extensions):
                zip_ref.extract(file_name, self.extract_path)

    def unpack_zipfile(self):
        """
        Opens and extracts the files.
        """
        try:
            with zipfile.ZipFile(self.source_path, "r") as zip_ref:
                self.extract_directories(zip_ref)
                self.extract_files(zip_ref)
                self.logger.info(
                    f"All files extracted and processed from {self.source_path} to {self.extract_path}."
                )
        except (FileNotFoundError, KeyError, TypeError, BadZipfile) as error:
            self.logger.error(f"An error occurred while uploading the file: {error}")
