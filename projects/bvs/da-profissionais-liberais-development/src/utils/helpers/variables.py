# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: variables.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code centralizes all variables to use in other modules
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from __future__ import annotations

import importlib
import inspect
import os
import random
import sys
from collections.abc import Generator
from datetime import datetime
from typing import Any
from typing import NamedTuple
from typing import Optional

import pytz

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


UTILS_MOD_PATH = get_module_path("src.utils.helpers.utils")
COLUMNS_MOD_PATH = get_module_path("src.utils.dataframe.pyspark.schema.columns")
LOGGER_MOD_PATH = get_module_path("src.utils.helpers.logger")

Columns = importlib.import_module(COLUMNS_MOD_PATH).Columns
Logger = importlib.import_module(LOGGER_MOD_PATH).Logger
Utils = importlib.import_module(UTILS_MOD_PATH).Utils


class TasksInfos(NamedTuple):
    """
    A named tuple representing information about tasks.

    Attributes:
        dir_name (str): The name of the directory related to the tasks.
        table_infos (Optional[dict], optional): Information about the table related to the tasks. Defaults to None.
        df_params (Optional[tuple], optional): Parameters for the dataframe related to the tasks. Defaults to None.
    """

    dir_name: str
    table_infos: Optional[dict] = None
    df_params: Optional[tuple] = None


class Variables(enumerate):
    """'
    Class to set all support variables.
    """

    @staticmethod
    def _get_all_variables() -> list:
        """
        Retrieves all class-level variables of the Variables class.

        Returns:
            list: A list of tuples, each representing a class-level variable and its value.
        """
        attributes = inspect.getmembers(
            Variables, lambda attr: not (inspect.isroutine(attr))
        )
        return [
            v for v in attributes if not (v[0].startswith("__") and v[0].endswith("__"))
        ]

    @staticmethod
    def _get_current_date(timezone) -> Generator[str, Any, None]:
        """
        This function returns a formatted according to the formats specified.

        Returns:
            tuple[str]: A tuple formatted dates as strings.
        """
        current_date = datetime.now(timezone)
        return (current_date.strftime(fmt) for fmt in ("%Y", "%m", "%d"))

    TIMEZONE_BR = pytz.timezone("America/Sao_Paulo")
    YEAR, MONTH, DAY = _get_current_date(TIMEZONE_BR)

    CHAR_WITH_ACCENTS: str = "áéíóúÁÉÍÓÚâêîôûÂÊÎÔÛàèìòùÀÈÌÒÙãõÃÕçÇäëïöüÄËÏÖÜ"
    CHAR_WITHOUT_ACCENTS: str = "aeiouAEIOUaeiouAEIOUaeiouAEIOUaoAOcCaeiouAEIOU"
    URL_CFC: str = (
        "https://www3.cfc.org.br/spw/apis/consultacadastralcfc/cfc/Listar{"
        "}?DefaultSort=&Filter=%5B%5B%5B%27EstadoConselho%27,%27contains%27,"
        "%27{}%27%5D%5D%5D&Group=&GroupSummary=&IsCountQuery=false&PaginateViaPrimaryKey=false&PreSelect"
        "=&PrimaryKey=&RemoteGrouping=false&RemoteSelect=false&RequireGroupCount=false&RequiteTotalCount"
        "=false&Select=&Skip={}&Sort=&StringToLower=false&Take={}&TotalSummary=&Conselho=&="
    )
    URL_SUSEP: str = (
        "https://www2.susep.gov.br/safe/corretoresapig/dadospublicos/pesquisar?tipoPessoa={}&page={}"
    )
    URL_CONFEF: str = (
        "https://www.confef.org.br/confef/registrados/ssp.registrados.php?draw=6&columns%5B0%5D%5Bdata%5D=0&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D={}&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=1&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=2&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=3&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=4&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=5&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=6&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=true&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=7&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=true&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=2&order%5B0%5D%5Bdir%5D=asc&start={}&length={}&search%5Bvalue%5D=&search%5Bregex%5D=false&_=1707231869155"
    )
    URL_CFFA: str = (
        "https://fonoaudiologia.org.br/fonoaudiologos/especialista-por-area/"
    )

    FILENAME: str = f"{{}}/{YEAR}{MONTH}{DAY}_{{}}.csv"
    BLOB_NAME: str = f"{{}}/{{}}/ano={YEAR}/mes={MONTH}/dia={DAY}/{{}}"
    STAGING_NAME: str = "staging"
    TRANSIENT_NAME: str = "transient"
    TRUSTED_NAME: str = "trusted"
    DIR_NAME_CFC: str = "conselho-federal-contabilidade"
    DIR_NAME_SUSEP: str = "superintendencia-seguros-privados"
    DIR_NAME_CONFEF: str = "conselho-federal-educacao-fisica"
    DIR_NAME_CFFA: str = "conselho-federal-fonoaudiologia"

    TRANSFORMER_TASKS: dict = {
        "transform-cfc": TasksInfos(
            dir_name=DIR_NAME_CFC,
            table_infos=Columns.TABLE_INFOS_CFC,
            df_params=("csv", ";", True, "UTF-8", "append"),
        ),
        "transform-susep": TasksInfos(
            dir_name=DIR_NAME_SUSEP,
            table_infos=Columns.TABLE_INFOS_SUSEP,
            df_params=("csv", ";", True, "UTF-8", "append"),
        ),
        "transform-confef": TasksInfos(
            dir_name=DIR_NAME_CONFEF,
            table_infos=Columns.TABLE_INFOS_CONFEF,
            df_params=("csv", ";", True, "UTF-8", "append"),
        ),
        "transform-cffa": TasksInfos(
            dir_name=DIR_NAME_CFFA,
            table_infos=Columns.TABLE_INFOS_CFFA,
            df_params=("csv", ";", True, "UTF-8", "append"),
        ),
    }

    LOADER_TASKS: dict = {
        "load-cfc": TasksInfos(
            dir_name=DIR_NAME_CFC,
            table_infos=Columns.TABLE_INFOS_CFC,
            df_params=("parquet", ";", True, "UTF-8", "append"),
        ),
        "load-susep": TasksInfos(
            dir_name=DIR_NAME_SUSEP,
            table_infos=Columns.TABLE_INFOS_SUSEP,
            df_params=("parquet", ";", True, "UTF-8", "append"),
        ),
        "load-confef": TasksInfos(
            dir_name=DIR_NAME_CONFEF,
            table_infos=Columns.TABLE_INFOS_CONFEF,
            df_params=("parquet", ";", True, "UTF-8", "append"),
        ),
        "load-cffa": TasksInfos(
            dir_name=DIR_NAME_CFFA,
            table_infos=Columns.TABLE_INFOS_CFFA,
            df_params=("parquet", ";", True, "UTF-8", "append"),
        ),
    }

    LOADER_BIGTABLE_TASKS: dict = {
        "load-bigtable-cfc": TasksInfos(dir_name=DIR_NAME_CFC),
    }

    UF: list = [
        "AC",
        "AL",
        "AP",
        "AM",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MT",
        "MS",
        "MG",
        "PA",
        "PB",
        "PR",
        "PE",
        "PI",
        "RJ",
        "RN",
        "RS",
        "RO",
        "RR",
        "SC",
        "SE",
        "TO",
        "SP",
    ]

    USER_AGENTS: list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:53.0) Gecko/20100101 Firefox/53.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/603.2.4 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.4",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/603.2.4 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.4",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/604.3.5 (KHTML, like Gecko) Version/11.0.1 Safari/604.3.5",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/604.4.7 (KHTML, like Gecko) Version/11.0.2 Safari/604.4.7",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/604.5.6 (KHTML, like Gecko) Version/11.0.3 Safari/604.5.6",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.3 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_16) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
    ]

    HEADERS = {"User-Agent": random.choice(USER_AGENTS)}  # nosec


if __name__ == "__main__":
    logger = Logger()
    utils = Utils(logger)
    utils.check_variables(Variables)
