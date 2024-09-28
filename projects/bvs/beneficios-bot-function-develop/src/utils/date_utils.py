#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: date_utils.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for handling objects of dates
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import calendar
from datetime import date
from dateutil.relativedelta import relativedelta


class CurrentDate:
    """
    Get the current date.
    """

    def __init__(self):
        pass

    @staticmethod
    def get_today() -> date:
        """
        Get the current date.
        Returns:
            date: The current date.
        """
        return date.today()


class MonthlyDateHandle:
    """
    Get the target month.
    """

    def __init__(self, current_date: CurrentDate):
        self.current_date = current_date

    def get_first_day(self) -> date:
        """
        Get the first day of the month.
        Returns:
            date: The first day of the month.
        """
        today = self.current_date
        return today.replace(day=1)

    @staticmethod
    def get_target_month(first_day_of_month, number_of_months) -> date:
        """
        Get the target month.
        Returns:
            date: The target month.
        """
        return first_day_of_month - relativedelta(months=number_of_months)

    @staticmethod
    def get_count_days_in_month(target_month) -> int:
        """
        Get the number of days in the target month.
        Returns:
            int: The number of days in the target month.
        """
        return calendar.monthrange(target_month.year, target_month.month)[1]

    @staticmethod
    def get_last_day_of_month(target_month, days_in_month) -> date:
        """
        Get the last day of the target month.
        Returns:
            date: The last day of the target month.
        """
        return target_month.replace(day=days_in_month)

    def set_format_date(self, target_date, format_date) -> str:
        """
        Set the format of the target date.
        Returns:
            str: The format of the target date.
        """
        return target_date.strftime(format_date)

    def get_target_month_date(self, number_of_months, format_date):
        """
        Get the previous month.
        Returns:
            str: The previous month.
        """
        first_day_of_month = self.get_first_day()
        target_month = self.get_target_month(first_day_of_month, number_of_months)
        days_in_month = self.get_count_days_in_month(target_month)
        last_day_of_month = self.get_last_day_of_month(target_month, days_in_month)
        target_monthly_date = self.set_format_date(last_day_of_month, format_date)
        return target_monthly_date


class QuarterDateHandle:
    """
    Get the target quarter.
    """

    def __init__(self, current_date: CurrentDate):
        self.current_date = current_date

    def get_current_year(self) -> int:
        """
        Get the current year in the format "yyyy".

        Returns:
            int: The current year in the format "yyyy".
        """
        today = self.current_date
        current_year = today.year
        return current_year

    def get_quarter_number(self) -> int:
        """
        Get the number of the current quarter.
        Returns:
            int: The number of the current quarter (1, 2, 3 or 4).
        """
        today = self.current_date
        quarter_start_month = (today.month - 1) // 3 * 3 + 1
        quarter_number = (quarter_start_month - 1) // 3 + 1
        return quarter_number

    @staticmethod
    def get_target_date(current_year, current_quarter, index) -> date:
        """
        Get the target date.
        Returns:
            date: The target date.
        """
        target_date = date(
            int(current_year), (int(current_quarter) - 1) * 3 + 1, 1
        ) - relativedelta(months=index * 3)
        return target_date

    @staticmethod
    def get_target_year(target_date) -> str:
        """
        Get the target year.
        Returns:
            str: The target year.
        """
        return target_date.year

    @staticmethod
    def get_target_quarter(target_date) -> str:
        """
        Get the target quarter.
        Returns:
            str: The target quarter.
        """
        target_quarter = (target_date.month - 1) // 3 + 1
        return target_quarter

    @staticmethod
    def set_format_date(target_year, target_quarter):
        """
        Set the format of the target date.
        Returns:
            str: The format of the target date.
        """
        return f"{target_year}{target_quarter:02}"

    def get_target_quarter_date(self, index) -> str:
        """
        Get the previous quarter.
        Returns:
            str: The previous quarter.
        """
        current_year = self.get_current_year()
        current_quarter = self.get_quarter_number()
        target_date = self.get_target_date(current_year, current_quarter, index)
        target_year = self.get_target_year(target_date)
        target_quarter = self.get_target_quarter(target_date)
        target_quarter_date = self.set_format_date(target_year, target_quarter)
        return target_quarter_date


class DailyDateHandle:
    """
    Get the target day.
    """

    def __init__(self, current_date: CurrentDate):
        self.current_date = current_date

    def get_target_day(self, number_of_days):
        """
        Get the target day.
        Returns:
            date: The target day.
        """
        today = self.current_date
        target_day = today - relativedelta(days=number_of_days)
        return target_day

    def set_format_date(self, target_day, format_date) -> str:
        """
        Set the format of the target date.
        Returns:
            str: The format of the target date.
        """
        return target_day.strftime(format_date)

    def get_target_day_date(self, number_of_days, format_date):
        """
        Get the target day date.
        Returns:
            str: The target day date.
        """
        target_day = self.get_target_day(number_of_days)
        target_daily_date = self.set_format_date(target_day, format_date)
        return target_daily_date


class YearlyDateHandle:
    """
    Get the target year.
    """

    def __init__(self, current_date: CurrentDate):
        self.current_date = current_date

    def get_target_year(self, number_of_years):
        """
        Get the target year.
        Returns:
            date: The target year.
        """
        today = self.current_date
        target_year = today - relativedelta(years=number_of_years)
        return target_year

    def set_format_date(self, target_year, format_date) -> str:
        """
        Set the format of the target date.
        Returns:
            str: The format of the target date.
        """
        return target_year.strftime(format_date)

    def get_target_year_date(self, number_of_years, format_date):
        """
        Get the target year date.
        Returns:
            str: The target year date.
        """
        target_year = self.get_target_year(number_of_years)
        target_year_date = self.set_format_date(target_year, format_date)
        return target_year_date


class DateUtils:
    """
    Get the target date.
    """

    def __init__(
        self,
        daily_date_handle=DailyDateHandle,
        monthly_date_handle=MonthlyDateHandle,
        quarter_date_handle=QuarterDateHandle,
        yearly_date_handle=YearlyDateHandle,
    ):
        self.daily_date_handle = daily_date_handle
        self.monthly_date_handle = monthly_date_handle
        self.quarter_date_handle = quarter_date_handle
        self.yearly_date_handle = yearly_date_handle
        self.use_date_daily = ["SANCOES", "IBAMA", "SERVIDORES/ESTADUAIS/PR"]
        self.use_date_quarter = ["DEBITOSTRABALHISTAS"]
        self.use_date_year = [
            "SERVIDORES/ESTADUAIS/STAGING/DF/DESPESA",
            "SERVIDORES/ESTADUAIS/STAGING/DF/RECEITA",
            "SERVIDORES/ESTADUAIS/STAGING/DF/LICITACAO",
        ]
        self.format_daily = "%Y%m%d"
        self.format_monthly = "%Y%m"
        self.format_yearly = "%Y"

    def get_target_date(self, blob_path, period):
        """
        Get the target date.
        Returns:
            str: The target date.
        """
        if any(
            blob_path.startswith(snippet_blob_name)
            for snippet_blob_name in self.use_date_daily
        ):
            target_daily_date = self.daily_date_handle.get_target_day_date(
                period, self.format_daily
            )
            return target_daily_date
        if any(
            blob_path.startswith(snippet_blob_name)
            for snippet_blob_name in self.use_date_quarter
        ):
            target_quarter_date = self.quarter_date_handle.get_target_quarter_date(
                period
            )
            return target_quarter_date
        if any(
            blob_path.startswith(snippet_blob_name)
            for snippet_blob_name in self.use_date_year
        ):
            target_year_date = self.yearly_date_handle.get_target_year_date(
                period, self.format_yearly
            )
            return target_year_date
        else:
            target_month_date = self.monthly_date_handle.get_target_month_date(
                period, self.format_monthly
            )
            return target_month_date
