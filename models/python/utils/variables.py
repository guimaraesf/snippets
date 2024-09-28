# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: variables.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This code centralizes all variables to use in other modules
# ================================================================================================
from __future__ import annotations
import inspect
from enum import Enum


class Variables(Enum):
    """
    Class to set all support variables.
    """

    @classmethod
    def init(cls):
        return {var.name: var.value for var in cls}

    @classmethod
    def get_all_variables(cls) -> list:
        """
        Retrieves all class-level variables of the Variables class.

        Returns:
            list: A list of tuples, each representing a class-level variable and its value.
        """
        attributes = inspect.getmembers(cls, lambda attr: not (inspect.isroutine(attr)))
        return [
            v for v in attributes if not (v[0].startswith("__") and v[0].endswith("__"))
        ]

    @classmethod
    def check_variables(cls) -> None:
        """
        Check if all variables defined in the Variables class have been initialized.
        """
        for key, value in cls.get_all_variables():
            if not value:
                print(
                    f"The variable {key} was not created, because the value is: '{value}'"
                )

    NAME_VAR: str = ""


if __name__ == "__main__":
    Variables.check_variables()
