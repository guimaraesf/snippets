# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: columns.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code centralizes all columns to use in spark schemas.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from typing import NamedTuple
from typing import Optional

from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType


class DataframeParams(NamedTuple):
    """
    A NamedTuple for holding parameters related to a DataFrame.

    Args:
        schema_params (tuple): A tuple containing parameters related to the schema of the DataFrame.
        regex (tuple): A tuple containing regular expression STRIP_NON_ALPHANUMERICs for data processing.
    """

    schema_params: tuple
    regex: tuple
    columns_to_row_key: Optional[list] = None


class Columns(enumerate):
    """
    Class to set all columns for schema on DataFrames.
    """

    string_type = StringType()
    timestamp_type = TimestampType()
    float_type = FloatType()
    integer_type = IntegerType()
    STRIP_NON_ALPHANUMERIC = r"[^\w\s]"

    TABLE_INFOS_CONFEF: dict = {
        "estado": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Estado do conselho profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "registro": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Registro do profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r" ")),
        ),
        "nome": DataframeParams(
            schema_params=(string_type, True, {"description": "Nome do profissional"}),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "sigla_registro": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Sigla do registro do profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r" ")),
        ),
        "categoria": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Categoria profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "natureza_titulo": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Natureza do título profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r" ")),
        ),
        "indice": DataframeParams(
            schema_params=(string_type, True, {"description": "Número do índice"}),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "codigo_registro": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Número de registro do profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r" ")),
        ),
        # "row_key": DataframeParams(
        #     schema_params=(
        #         string_type,
        #         True,
        #         {"description": "Chave de linha única para cada registro"},
        #     ),
        #     regex=((r"", r"")),
        #     columns_to_row_key=[
        #         "estado",
        #         "categoria",
        #         "natureza_titulo",
        #         "registro",
        #         "sigla_registro",
        #     ],
        # ),
    }

    TABLE_INFOS_SUSEP: dict = {
        "corretor_id": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Identificador único do corretor"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "cpf_cnpj": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "CPF ou CNPJ do corretor"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "protocolo": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Número de protocolo do corretor"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "nome": DataframeParams(
            schema_params=(string_type, True, {"description": "Nome do corretor"}),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "recadastrado": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Status de recadastramento do corretor"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "situacao": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Situação atual do corretor"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "produtos": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Produtos que o corretor está autorizado a vender"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        # "row_key": DataframeParams(
        #     schema_params=(
        #         string_type,
        #         True,
        #         {"description": "Chave de linha única para cada registro"},
        #     ),
        #     regex=((r"", r"")),
        #     columns_to_row_key=[],
        # ),
    }

    TABLE_INFOS_CFC: dict = {
        "chave": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Chave única para cada registro"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r" ")),
        ),
        "nome": DataframeParams(
            schema_params=(string_type, True, {"description": "Nome do profissional"}),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "registro": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Número de registro do profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r" ")),
        ),
        "estado_conselho": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Estado do conselho profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r" ")),
        ),
        "cpf_cnpj": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "CPF ou CNPJ do profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "descricao_categoria": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Descrição da categoria profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "descricao_tipo_registro": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Descrição do tipo de registro"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "situacao_cadastral": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Situação cadastral do profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        # "row_key": DataframeParams(
        #     schema_params=(
        #         string_type,
        #         True,
        #         {"description": "Chave de linha única para cada registro"},
        #     ),
        #     regex=((r"", r"")),
        #     columns_to_row_key=["estado_conselho", "registro", "cpf_cnpj"],
        # ),
    }

    TABLE_INFOS_CFFA: dict = {
        "numero": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Número de identificação do profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "crfa": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Registro CRFA do profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "nome": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Nome do profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r" ")),
        ),
        "especialidade": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Especialidade do profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r" ")),
        ),
        "estado": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Estado do conselho profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "cidade": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Cidade do conselho profissional"},
            ),
            regex=((STRIP_NON_ALPHANUMERIC, r"")),
        ),
        "validade": DataframeParams(
            schema_params=(
                string_type,
                True,
                {"description": "Validade do registro do profissional"},
            ),
            regex=((r"/", r"-")),
        ),
        # "row_key": DataframeParams(
        #     schema_params=(
        #         string_type,
        #         True,
        #         {"description": "Chave de linha única para cada registro"},
        #     ),
        #     regex=((r"", r"")),
        #     columns_to_row_key=["estado", "cidade", "especialidade", "crfa"],
        # ),
    }

    INFO_CONTROL_COLUMNS: dict = {
        "arquivo": (string_type, True, {"description": "Nome do arquivo de origem"}),
        "data_processamento": (
            string_type,
            True,
            {"description": "Data em que o processamento foi executado"},
        ),
    }
