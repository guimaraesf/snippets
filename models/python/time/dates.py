# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# ================================================================================================
# Module: dates.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This module is responsible for handling objects of dates
# ================================================================================================
from datetime import date
from typing import Any, Generator


class Dates:
    def __init__(self):
        self.formats = ("%Y", "%m", "%d")  # (e.g., year=2023, month=11, day=24)

    @staticmethod
    def get_today() -> date:
        """
        Get the current date.

        Returns:
            date: The current date.
        """
        return date.today()

    def get_dates(self) -> Generator[str, Any, None]:
        """
        This function returns a formatted according to the formats specified.

        Returns:
            tuple[str]: A tuple formatted dates as strings.
        """
        today = self.get_today()
        return (today.strftime(fmt) for fmt in self.formats)
