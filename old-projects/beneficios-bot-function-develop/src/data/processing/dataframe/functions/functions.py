#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: functions.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code is responsible for applying all the necessary transformation steps
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
from pyspark.sql.functions import regexp_replace, col, lit, concat, substring
# pylint: disable=import-error
from cleaning import Cleaning, Formatting
# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))

class Functions:
    """
    This class is responsible for applying all the necessary transformation steps
    """

    def __init__(self, cleaning=Cleaning(), formatting=Formatting()):
        self.cleaning = cleaning
        self.formatting = formatting

    # SANCÕES ================================================================
    def columns_ceis(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CADASTRO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "CODIGO_DA_SANCAO": [("[^a-zA-Z0-9 ]", " ")],
            "TIPO_DE_PESSOA": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "CPF_OU_CNPJ_DO_SANCIONADO": [("[^a-zA-Z0-9 ]", " ")],
            "NOME_DO_SANCIONADO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "NOME_INFORMADO_PELO_ORGAO_SANCIONADOR": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "RAZAO_SOCIAL_CADASTRO_RECEITA": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "NOME_FANTASIA_CADASTRO_RECEITA": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "NUMERO_DO_PROCESSO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "CATEGORIA_DA_SANCAO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "DATA_INICIO_SANCAO": [("[^a-zA-Z0-9 ]", " ")],
            "DATA_FINAL_SANCAO": [("[^a-zA-Z0-9 ]", " ")],
            "DATA_PUBLICACAO": [("[^a-zA-Z0-9 ]", " ")],
            "PUBLICACAO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "DETALHAMENTO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "DATA_DO_TRANSITO_EM_JULGADO": [("[^a-zA-Z0-9 ]", " ")],
            "ABRAGENCIA_DEFINIDA_EM_DECISAO_JUDICIAL": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "ORGAO_SANCIONADOR": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "UF_ORGAO_SANCIONADOR": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "FUNDAMENTACAO_LEGAL": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
        }

        date_columns = [
            "DATA_INICIO_SANCAO",
            "DATA_FINAL_SANCAO",
            "DATA_PUBLICACAO",
            "DATA_DO_TRANSITO_EM_JULGADO",
        ]

        start1 = 7
        len1 = 4

        start2 = 4
        len2 = 2

        start3 = 0
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    def columns_cepim(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CNPJ_ENTIDADE": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "NOME_ENTIDADE": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "NUMERO_CONVENIO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "ORGAO_CONCEDENTE": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "MOTIVO_DO_IMPEDIMENTO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_cnep(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CADASTRO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "CODIGO_DA_SANCAO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "TIPO_DE_PESSOA": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "CPF_OU_CNPJ_DO_SANCIONADO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "NOME_DO_SANCIONADO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "NOME_INFORMADO_PELO_ORGAO_SANCIONADOR": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "RAZAO_SOCIAL_CADASTRO_RECEITA": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "NOME_FANTASIA_CADASTRO_RECEITA": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "NUMERO_DO_PROCESSO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "CATEGORIA_DA_SANCAO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "VALOR_DA_MULTA": [("[,]", ".")],
            "DATA_INICIO_SANCAO": [("[^A-Za-z0-9 ]", " ")],
            "DATA_FINAL_SANCAO": [("[^A-Za-z0-9 ]", " ")],
            "DATA_PUBLICACAO": [("[^A-Za-z0-9 ]", " ")],
            "PUBLICACAO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "DETALHAMENTO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "DATA_DO_TRANSITO_EM_JULGADO": [("[^A-Za-z0-9 ]", " ")],
            "ABRAGENCIA_DEFINIDA_EM_DECISAO_JUDICIAL": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "ORGAO_SANCIONADOR": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "UF_ORGAO_SANCIONADOR": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "FUNDAMENTACAO_LEGAL": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
        }

        date_columns = [
            "DATA_INICIO_SANCAO",
            "DATA_FINAL_SANCAO",
            "DATA_PUBLICACAO",
            "DATA_DO_TRANSITO_EM_JULGADO",
        ]

        start1 = 7
        len1 = 4

        start2 = 4
        len2 = 2

        start3 = 0
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    def columns_acordos_leniencia(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ID_DO_ACORDO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "CNPJ_DO_SANCIONADO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "RAZAO_SOCIAL_CADASTRO_RECEITA": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "NOME_FANTASIA_CADASTRO_RECEITA": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "DATA_DE_INICIO_DO_ACORDO": [("[^A-Za-z0-9 ]", " ")],
            "DATA_DE_FIM_DO_ACORDO": [("[^A-Za-z0-9 ]", " ")],
            "SITUACAO_DO_ACORDO_DE_LENIENICA": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "DATA_DA_INFORMACAO": [("[^A-Za-z0-9 ]", " ")],
            "NUMERO_DO_PROCESSO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "TERMOS_DO_ACORDO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "ORGAO_SANCIONADOR": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
        }

        date_columns = [
            "DATA_DE_INICIO_DO_ACORDO",
            "DATA_DE_FIM_DO_ACORDO",
            "DATA_DA_INFORMACAO",
        ]

        start1 = 7
        len1 = 4

        start2 = 4
        len2 = 2

        start3 = 0
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    def columns_efeitos_leniencia(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ID_DO_ACORDO": [("[^A-Za-z0-9 ]", " ")],
            "EFEITO_DO_ACORDO_DE_LENIENCIA": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
            "COMPLEMENTO": [
                ("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|(?:[^\\p{L}\\p{N}]+)|( {2,})", " ")
            ],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    # IBAMA ==================================================================
    def columns_ibama(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "SEQ_AUTO_INFRACAO": [("[^A-Za-z0-9 ]", "")],
            "NUM_AUTO_INFRACAO": [("[^A-Za-z0-9 ]", "")],
            "SER_AUTO_INFRACAO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_AUTO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "TIPO_MULTA": [("[^A-Za-z0-9 ]", "")],
            "VAL_AUTO_INFRACAO": [("[,]", ".")],
            "PATRIMONIO_APURACAO": [("[^A-Za-z0-9 ]", "")],
            "GRAVIDADE_INFRACAO": [("[^A-Za-z0-9 ]", "")],
            "UNID_ARRECADACAO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "DES_AUTO_INFRACAO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "DAT_HORA_AUTO_INFRACAO": [("[^A-Za-z0-9 ]", "")],
            "FORMA_ENTREGA": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "DAT_CIENCIA_AUTUACAO": [("[^A-Za-z0-9 ]", "")],
            "COD_MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "MUNICIPIO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "UF": [("[^A-Za-z0-9 ]", "")],
            "NUM_PROCESSO": [("[^A-Za-z0-9 ]", "")],
            "COD_INFRACAO": [("[.0]", "")],
            "DES_INFRACAO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "TIPO_INFRACAO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "NOME_INFRATOR": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "CPF_CNPJ_INFRATOR": [("[^A-Za-z0-9 ]", "")],
            "QTD_AREA": [("[,]", ".")],
            "INFRACAO_AREA": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "DES_OUTROS_TIPO_AREA": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "CLASSIFICACAO_AREA": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "NUM_LATITUDE_AUTO": [("[,]", ".")],
            "NUM_LONGITUDE_AUTO": [("[,]", ".")],
            "DES_LOCAL_INFRACAO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "NOTIFICACAO_VINCULADA": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "ACAO_FISCALIZATORIA": [("[^A-Za-z0-9 ]", "")],
            "UNID_CONTROLE": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "TIPO_ACAO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "OPERACAO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "DENUNCIA_SISLIV": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "ORDEM_FISCALIZACAO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "SOLICITACAO_RECURSO": [("[^A-Za-z0-9 ]", "")],
            "OPERACAO_SOL_RECURSO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "DAT_LANCAMENTO": [("[^A-Za-z0-9 ]", "")],
            "DAT_ULT_ALTERACAO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_ULT_ALTERACAO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "ULTIMA_ATUALIZACAO_RELATORIO": [("[^A-Za-z0-9 ]", "")],
        }

        date_columns = [
            "DAT_HORA_AUTO_INFRACAO",
            "DAT_CIENCIA_AUTUACAO",
            "DAT_LANCAMENTO",
            "DAT_ULT_ALTERACAO",
            "ULTIMA_ATUALIZACAO_RELATORIO",
        ]

        start1 = 5
        len1 = 4

        start2 = 3
        len2 = 2

        start3 = 1
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    # BENEFÍCIOS SOCIAIS =====================================================
    def columns_auxilio_brasil(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "MES_COMPETENCIA": [("[^A-Za-z0-9 ]", "")],
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "UF": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MUNICIPIO_SIAFI": [("[^A-Za-z0-9 ]", "")],
            "NOME_MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "CPF_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NIS_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NOME_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "VALOR_PARCELA": [("[.]", ""), ("[,]", ".")],
        }

        date_columns = ["MES_REFERENCIA", "MES_COMPETENCIA"]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=df_for_clean,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2
        )

        return dataframe

    def columns_auxilio_emergencial(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "MES_DISPONIBILIZACAO": [("[^A-Za-z0-9 ]", "")],
            "UF": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MUNICIPIO_IBGE": [("[^A-Za-z0-9 ]", "")],
            "NOME_MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "NIS_BENEFICIARIO": [("[^A-Za-z0-9 ]", "")],
            "CPF_BENEFICIARIO": [("[^A-Za-z0-9 ]", "")],
            "NOME_BENEFICIARIO": [("[^A-Za-z0-9 ]", "")],
            "NIS_RESPONSAVEL": [("[^A-Za-z0-9 ]", "")],
            "CPF_RESPONSAVEL": [("[^A-Za-z0-9 ]", "")],
            "NOME_RESPONSAVEL": [("[A-Za-z0-9 ][À-Üà-ü][ç]", "")],
            "ENQUADRAMENTO": [("[^A-Za-z0-9 ][À-Üà-ü][ç]", "")],
            "PARCELA": [("[^A-Za-z0-9 ]", "")],
            "OBSERVACAO": [("[A-Za-z0-9 ][À-Üà-ü][ç]", "")],
            "VALOR_BENEFICIO": [("[^A-Za-z][0,9][.]{2}", ""), ("[,]", ".")],
        }

        date_columns = ["MES_DISPONIBILIZACAO"]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=df_for_clean,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2
        )

        return dataframe

    def columns_bolsafamilia_saques(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "MES_COMPETENCIA": [("[^A-Za-z0-9 ]", "")],
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "UF": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MUNICIPIO_SIAFI": [("[^A-Za-z0-9 ]", "")],
            "NOME_MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "CPF_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NIS_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NOME_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "DATA_SAQUE": [("[^A-Za-z0-9 ]", "")],
            "VALOR_PARCELA": [("[^A-Za-z][0,9][.]{2}", ""), ("[,]", ".")],
        }

        date_columns_1 = ["MES_COMPETENCIA", "MES_REFERENCIA"]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        date_columns_2 = ["DATA_SAQUE"]

        start3 = 5
        len3 = 4

        start4 = 3
        len4 = 2

        start5 = 1
        len5 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=df_for_clean,
            columns=date_columns_1,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=df_for_clean,
            columns=date_columns_2,
            start1=start3,
            len1=len3,
            start2=start4,
            len2=len4,
            start3=start5,
            len3=len5
        )

        return dataframe

    def columns_bolsafamilia_pagamentos(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "MES_COMPETENCIA": [("[^A-Za-z0-9 ]", "")],
            "UF": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MUNICIPIO_SIAFI": [("[^A-Za-z0-9 ]", "")],
            "NOME_MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "CPF_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NIS_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NOME_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "VALOR_PARCELA": [("[^A-Za-z][0,9][.]{2}", ""), ("[,]", ".")],
        }

        date_columns = ["MES_REFERENCIA", "MES_COMPETENCIA"]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=df_for_clean,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2
        )

        return dataframe

    def columns_peti(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "UF": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MUNICIPIO_SIAFI": [("[^A-Za-z0-9 ]", "")],
            "NOME_MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "NIS_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NOME_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_BENEFICIARIO": [("[A-Za-z0-9 ][À-Üà-ü][ç]", "")],
            "VALOR_PARCELA": [("[^A-Za-z][0,9][.]{2}", ""), ("[,]", ".")],
        }

        date_columns = ["MES_REFERENCIA"]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=df_for_clean,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2
        )

        return dataframe

    def columns_bpc(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "MES_COMPETENCIA": [("[^A-Za-z0-9 ]", "")],
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "UF": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MUNICIPIO_SIAFI": [("[^A-Za-z0-9 ]", "")],
            "NOME_MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "NIS_BENEFICIARIO": [("[^A-Za-z0-9 ]", "")],
            "CPF_BENEFICIARIO": [("[^A-Za-z0-9 ]", "")],
            "NOME_BENEFICIARIO": [("[^A-Za-z0-9 ]", "")],
            "NIS_REPRESENTANTE_LEGAL": [("[^A-Za-z0-9 ]", "")],
            "CPF_REPRESENTANTE_LEGAL": [("[^A-Za-z0-9 ]", "")],
            "NOME_REPRESENTANTE_LEGAL": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_BENEFICIO": [("[^A-Za-z0-9 ]", "")],
            "BENEFICIO_CONCEDIDO_JUDICIALMENTE": [("[A-Za-z0-9 ][À-Üà-ü][ç]", "")],
            "VALOR_PARCELA": [("[^A-Za-z][0,9][.]{2}", ""), ("[,]", ".")],
        }

        date_columns = ["MES_COMPETENCIA", "MES_REFERENCIA"]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=df_for_clean,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2
        )

        return dataframe

    def columns_garantia_safra(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "UF": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MUNICIPIO_SIAFI": [("[^A-Za-z0-9 ]", "")],
            "NOME_MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "NIS_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NOME_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "VALOR_PARCELA": [("[^A-Za-z][0,9][.]{2}", ""), ("[,]", ".")],
        }

        date_columns = ["MES_REFERENCIA"]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=df_for_clean,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2
        )

        return dataframe

    def columns_seguro_defeso(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "UF": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MUNICIPIO_SIAFI": [("[^A-Za-z0-9 ]", "")],
            "NOME_MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "CPF_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NIS_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "RGP_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NOME_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "VALOR_PARCELA": [("[^A-Za-z][0,9][.]{2}", ""), ("[,]", ".")],
        }

        date_columns = ["MES_REFERENCIA"]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=df_for_clean,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2
        )

        return dataframe

    # SEGURO DESEMPREGO ======================================================
    def columns_trab_formal(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "DATA_REQUERIMENTO": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_REQUERIMENTO": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "PIS_PASEP_NIT": [("[^A-Za-z0-9 ]", "")],
            "NOME_REQUERENTE": [("[^A-Za-z0-9 ]", "")],
            "UF_RECEPCAO": [("[^A-Za-z0-9 ]", "")],
            "MUNICIPIO_RECEPCAO": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "DATA_EMISSAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "DATA_SITUACAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "VALOR_SITUACAO_PARCELA": [("[.]", ""), ("[,]", ".")],
        }

        date_columns = [
            "DATA_REQUERIMENTO",
            "DATA_EMISSAO_PARCELA",
            "DATA_SITUACAO_PARCELA",
        ]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        start3 = 7
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    def columns_empr_domestico(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "DATA_REQUERIMENTO": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_REQUERIMENTO": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "PIS_PASEP_NIT": [("[^A-Za-z0-9 ]", "")],
            "NOME_REQUERENTE": [("[^A-Za-z0-9 ]", "")],
            "UF_RECEPCAO": [("[^A-Za-z0-9 ]", "")],
            "MUNICIPIO_RECEPCAO": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "DATA_EMISSAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "DATA_SITUACAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "VALOR_SITUACAO_PARCELA": [("[.]", ""), ("[,]", ".")],
        }

        date_columns = [
            "DATA_REQUERIMENTO",
            "DATA_EMISSAO_PARCELA",
            "DATA_SITUACAO_PARCELA",
        ]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        start3 = 7
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    def columns_trab_resgatado(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "DATA_REQUERIMENTO": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_REQUERIMENTO": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "PIS_PASEP_NIT": [("[^A-Za-z0-9 ]", "")],
            "NOME_REQUERENTE": [("[^A-Za-z0-9 ]", "")],
            "UF_RECEPCAO": [("[^A-Za-z0-9 ]", "")],
            "MUNICIPIO_RECEPCAO": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "DATA_EMISSAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "DATA_SITUACAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "VALOR_SITUACAO_PARCELA": [("[.]", ""), ("[,]", ".")],
        }

        date_columns = [
            "DATA_REQUERIMENTO",
            "DATA_EMISSAO_PARCELA",
            "DATA_SITUACAO_PARCELA",
        ]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        start3 = 7
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    def columns_bolsa_qualific(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "DATA_REQUERIMENTO": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_REQUERIMENTO": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "PIS_PASEP_NIT": [("[^A-Za-z0-9 ]", "")],
            "NOME_REQUERENTE": [("[^A-Za-z0-9 ]", "")],
            "UF_RECEPCAO": [("[^A-Za-z0-9 ]", "")],
            "MUNICIPIO_RECEPCAO": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "DATA_EMISSAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "DATA_SITUACAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "VALOR_SITUACAO_PARCELA": [("[.]", ""), ("[,]", ".")],
        }

        date_columns = [
            "DATA_REQUERIMENTO",
            "DATA_EMISSAO_PARCELA",
            "DATA_SITUACAO_PARCELA",
        ]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        start3 = 7
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    def columns_pesc_artesanal(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "DATA_REQUERIMENTO": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_REQUERIMENTO": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "PIS_PASEP_NIT": [("[^A-Za-z0-9 ]", "")],
            "RGP": [("[^A-Za-z0-9 ]", "")],
            "DEFESO": [("[^A-Za-zÀ-ÖØ-öø-ÿÇç0-9]+|( {2,})", " ")],
            "DATA_INICIO_DEFESO": [("[^A-Za-z0-9 ]", "")],
            "DATA_FIM_DEFESO": [("[^A-Za-z0-9 ]", "")],
            "NOME_REQUERENTE": [("[^A-Za-z0-9 ]", "")],
            "UF_RECEPCAO": [("[^A-Za-z0-9 ]", "")],
            "MUNICIPIO_RECEPCAO": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "DATA_EMISSAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "DATA_SITUACAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_PARCELA": [("[^A-Za-z0-9 ]", "")],
            "VALOR_SITUACAO_PARCELA": [("[.]", ""), ("[,]", ".")],
        }

        date_columns = [
            "DATA_REQUERIMENTO",
            "DATA_INICIO_DEFESO",
            "DATA_FIM_DEFESO",
            "DATA_EMISSAO_PARCELA",
            "DATA_SITUACAO_PARCELA",
        ]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        start3 = 7
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    # SERVIDORES PÚBLICOS ====================================================
    def columns_aposentados(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ID_SERVIDOR_PORTAL": [("[^A-Za-z0-9 ]", "")],
            "NOME": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "MATRICULA": [("[^A-Za-z0-9 ]", "")],
            "COD_TIPO_APOSENTADORIA": [("[^A-Za-z0-9 ]", "")],
            "TIPO_APOSENTADORIA": [("[^A-Za-z0-9 ]", " ")],
            "DATA_APOSENTADORIA": [("/", "-")],
            "DESCRICAO_CARGO": [("[^A-Za-z0-9 ]", " ")],
            "COD_UORG_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "UORG_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORG_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "ORG_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORGSUP_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "ORGSUP_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_TIPO_VINCULO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_VINCULO": [("[^A-Za-z0-9 ]", " ")],
            "SITUACAO_VINCULO": [("[^A-Za-z0-9 ]", " ")],
            "REGIME_JURIDICO": [("[^A-Za-z0-9 ]", "")],
            "JORNADA_DE_TRABALHO": [("[^A-Za-z0-9 ]", "")],
            "DATA_INGRESSO_CARGOFUNCAO": [("/", "-")],
            "DATA_NOMEACAO_CARGOFUNCAO": [("/", "-")],
            "DATA_INGRESSO_ORGAO": [("/", "-")],
            "DOCUMENTO_INGRESSO_SERVICO_PUBLICO": [("[^A-Za-z0-9 ]", "")],
            "DATA_DIPLOMA_INGRESSO_SERVICO_PUBLICO": [("/", "-")],
            "DIPLOMA_INGRESSO_CARGOFUNCAO": [("[^A-Za-z0-9 ]", "")],
            "DIPLOMA_INGRESSO_ORGAO": [("[^A-Za-z0-9 ]", "")],
            "DIPLOMA_INGRESSO_SERVICOPUBLICO": [("[^A-Za-z0-9 ]", "")],
            "ANO": [("[^A-Za-z0-9 ]", "")],
            "MES": [("[^A-Za-z0-9 ]", ""), ("[.0]", "")],
            "REMUNERACAO_BASICA_BRUTA_BRL": [("[,]", ".")],
            "REMUNERACAO_BASICA_BRUTA_USD": [("[,]", ".")],
            "ABATE_TETO_BRL": [("[,]", ".")],
            "ABATE_TETO_USD": [("[,]", ".")],
            "GRATIFICACAO_NATALINA_BRUTA_BRL": [("[,]", ".")],
            "GRATIFICACAO_NATALINA_BRUTA_USD": [("[,]", ".")],
            "ABATE_TETO_GRATIFICACAO_NATALINA_BRL": [("[,]", ".")],
            "ABATE_TETO_GRATIFICACAO_NATALINA_USD": [("[,]", ".")],
            "FERIAS_BRL": [("[,]", ".")],
            "FERIAS_USD": [("[,]", ".")],
            "OUTRAS_REMUNERACOES_EVENTUAIS_BRL": [("[,]", ".")],
            "OUTRAS_REMUNERACOES_EVENTUAIS_USD": [("[,]", ".")],
            "IRRF_BRL": [("[,]", ".")],
            "IRRF_USD": [("[,]", ".")],
            "PSS_RPGS_BRL": [("[,]", ".")],
            "PSS_RPGS_USD": [("[,]", ".")],
            "DEMAIS_DEDUCOES_BRL": [("[,]", ".")],
            "DEMAIS_DEDUCOES_USD": [("[,]", ".")],
            "PENSAO_MILITAR_BRL": [("[,]", ".")],
            "PENSAO_MILITAR_USD": [("[,]", ".")],
            "FUNDO_SAUDE_BRL": [("[,]", ".")],
            "FUNDO_SAUDE_USD": [("[,]", ".")],
            "TAXA_OCUPACAO_IMOVEL_FUNCIONAL_BRL": [("[,]", ".")],
            "TAXA_OCUPACAO_IMOVEL_FUNCIONAL_USD": [("[,]", ".")],
            "REMUNERACAO_APOS_DEDUCOES_OBRIGATORIAS_BRL": [("[,]", ".")],
            "REMUNERACAO_APOS_DEDUCOES_OBRIGATORIAS_USD": [("[,]", ".")],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_CIVIL_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_CIVIL_USD": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_MILITAR_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_MILITAR_USD": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_PROGRAMA_DESLIGAMENTO_VOLUNTARIO_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_PROGRAMA_DESLIGAMENTO_VOLUNTARIO_USD": [
                ("[,]", ".")
            ],
            "TOTAL_VERBAS_INDENIZATORIAS_BRL": [("[,]", ".")],
            "TOTAL_VERBAS_INDENIZATORIAS_USD": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_honorarios_advoc(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ANO": [("[^A-Za-z0-9 ]", "")],
            "MES": [("[^A-Za-z0-9 ]", "")],
            "ID_SERVIDOR_PORTAL": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "NOME": [("[^A-Za-z0-9 ]", "")],
            "OBSERVACOES": [("[^A-Za-z0-9 ]", "")],
            "VALOR": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_honorarios_jetons(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ANO": [("[^A-Za-z0-9 ]", "")],
            "MES": [("[^A-Za-z0-9 ]", "")],
            "ID_SERVIDOR_PORTAL": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "NOME": [("[^A-Za-z0-9 ]", "")],
            "OBSERVACOES": [("[^A-Za-z0-9 ]", "")],
            "VALOR": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_militares(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ID_SERVIDOR_PORTAL": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "NOME": [("[^A-Za-z0-9 ]", "")],
            "MATRICULA": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO_CARGO": [("[^A-Za-z0-9 ]", " ")],
            "CLASSE_CARGO": [("[^A-Za-z0-9 ]", "")],
            "REFERENCIA_CARGO": [("[^A-Za-z0-9 ]", "")],
            "PADRAO_CARGO": [("[^A-Za-z0-9 ]", "")],
            "SIGLA_FUNCAO": [("[^A-Za-z0-9 ]", "")],
            "NIVEL_FUNCAO": [("[^A-Za-z0-9 ]", "")],
            "FUNCAO": [("[^A-Za-z0-9 ]", " ")],
            "CODIGO_ATIVIDADE": [("[^A-Za-z0-9 ]", "")],
            "ATIVIDADE": [("[^A-Za-z0-9 ]", " ")],
            "OPCAO_PARCIAL": [("[^A-Za-z0-9 ]", "")],
            "COD_UORG_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "UORG_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORG_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "ORG_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORGSUP_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "ORGSUP_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_UORG_EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "UORG_EXERCICIO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORG_EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "ORG_EXERCICIO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORGSUP_EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "ORGSUP_EXERCICIO": [("[^A-Za-z0-9 ]", " ")],
            "COD_TIPO_VINCULO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_VINCULO": [("[^A-Za-z0-9 ]", " ")],
            "SITUACAO_VINCULO": [("[^A-Za-z0-9 ]", " ")],
            "REGIME_JURIDICO": [("[^A-Za-z0-9 ]", "")],
            "JORNADA_DE_TRABALHO": [("[^A-Za-z0-9 ]", "")],
            "DATA_INGRESSO_CARGOFUNCAO": [("/", "-")],
            "DATA_NOMEACAO_CARGOFUNCAO": [("/", "-")],
            "DATA_INGRESSO_ORGAO": [("/", "-")],
            "DOCUMENTO_INGRESSO_SERVICO_PUBLICO": [("[^A-Za-z0-9 ]", "")],
            "DATA_DIPLOMA_INGRESSO_SERVICO_PUBLICO": [("/", "-")],
            "DIPLOMA_INGRESSO_CARGOFUNCAO": [("[^A-Za-z0-9 ]", "")],
            "DIPLOMA_INGRESSO_ORGAO": [("[^A-Za-z0-9 ]", " ")],
            "DIPLOMA_INGRESSO_SERVICOPUBLICO": [("[^A-Za-z0-9 ]", " ")],
            "UF_EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "ANO": [("[^A-Za-z0-9 ]", "")],
            "MES": [("[^A-Za-z0-9 ]", ""), ("[.0]", "")],
            "REMUNERACAO_BASICA_BRUTA_BRL": [("[,]", ".")],
            "REMUNERACAO_BASICA_BRUTA_USD": [("[,]", ".")],
            "ABATE_TETO_BRL": [("[,]", ".")],
            "ABATE_TETO_USD": [("[,]", ".")],
            "GRATIFICACAO_NATALINA_BRUTA_BRL": [("[,]", ".")],
            "GRATIFICACAO_NATALINA_BRUTA_USD": [("[,]", ".")],
            "ABATE_TETO_GRATIFICACAO_NATALINA_BRL": [("[,]", ".")],
            "ABATE_TETO_GRATIFICACAO_NATALINA_USD": [("[,]", ".")],
            "FERIAS_BRL": [("[,]", ".")],
            "FERIAS_USD": [("[,]", ".")],
            "OUTRAS_REMUNERACOES_EVENTUAIS_BRL": [("[,]", ".")],
            "OUTRAS_REMUNERACOES_EVENTUAIS_USD": [("[,]", ".")],
            "IRRF_BRL": [("[,]", ".")],
            "IRRF_USD": [("[,]", ".")],
            "PSS_RPGS_BRL": [("[,]", ".")],
            "PSS_RPGS_USD": [("[,]", ".")],
            "DEMAIS_DEDUCOES_BRL": [("[,]", ".")],
            "DEMAIS_DEDUCOES_USD": [("[,]", ".")],
            "PENSAO_MILITAR_BRL": [("[,]", ".")],
            "PENSAO_MILITAR_USD": [("[,]", ".")],
            "FUNDO_SAUDE_BRL": [("[,]", ".")],
            "FUNDO_SAUDE_USD": [("[,]", ".")],
            "TAXA_OCUPACAO_IMOVEL_FUNCIONAL_BRL": [("[,]", ".")],
            "TAXA_OCUPACAO_IMOVEL_FUNCIONAL_USD": [("[,]", ".")],
            "REMUNERACAO_APOS_DEDUCOES_OBRIGATORIAS_BRL": [("[,]", ".")],
            "REMUNERACAO_APOS_DEDUCOES_OBRIGATORIAS_USD": [("[,]", ".")],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_CIVIL_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_CIVIL_USD": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_MILITAR_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_MILITAR_USD": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_PROGRAMA_DESLIGAMENTO_VOLUNTARIO_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_PROGRAMA_DESLIGAMENTO_VOLUNTARIO_USD": [
                ("[,]", ".")
            ],
            "TOTAL_VERBAS_INDENIZATORIAS_BRL": [("[,]", ".")],
            "TOTAL_VERBAS_INDENIZATORIAS_USD": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_pensionistas(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ID_SERVIDOR_PORTAL": [("[^A-Za-z0-9 ]", "")],
            "NOME": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "MATRICULA": [("[^A-Za-z0-9 ]", "")],
            "CPF_REPRESENTANTE_LEGAL": [("[^A-Za-z0-9 ]", "")],
            "NOME_REPRESENTANTE_LEGAL": [("[^A-Za-z0-9 ]", " ")],
            "CPF_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", "")],
            "NOME_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_TIPO_PENSAO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_PENSAO": [("[^A-Za-z0-9 ]", " ")],
            "DATA_INICIO_PENSAO": [("/", "-")],
            "DESCRICAO_CARGO_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_UORG_LOTACAO_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", "")],
            "UORG_LOTACAO_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORG_LOTACAO_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", "")],
            "ORG_LOTACAO_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORGSUP_LOTACAO_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", "")],
            "ORGSUP_LOTACAO_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_TIPO_VINCULO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_VINCULO": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_VINCULO": [("[^A-Za-z0-9 ]", " ")],
            "REGIME_JURIDICO_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", "")],
            "JORNADA_DE_TRABALHO_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", "")],
            "DATA_INGRESSO_CARGOFUNCAO_INSTITUIDOR_PENSAO": [("/", "-")],
            "DATA_NOMEACAO_CARGOFUNCAO_INSTITUIDOR_PENSAO": [("/", "-")],
            "DATA_INGRESSO_ORGAO_INSTITUIDOR_PENSAO": [("/", "-")],
            "DOCUMENTO_INGRESSO_SERVICO_PUBLICO_INSTITUIDOR_PENSAO": [
                ("[^A-Za-z0-9 ]", "")
            ],
            "DATA_DIPLOMA_INGRESSO_SERVICO_PUBLICO_INSTITUIDOR_PENSAO": [("/", "-")],
            "DIPLOMA_INGRESSO_CARGOFUNCAO_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", "")],
            "DIPLOMA_INGRESSO_ORGAO_INSTITUIDOR_PENSAO": [("[^A-Za-z0-9 ]", "")],
            "DIPLOMA_INGRESSO_SERVICOPUBLICO_INSTITUIDOR_PENSAO": [
                ("[^A-Za-z0-9 ]", "")
            ],
            "ANO": [("[^A-Za-z0-9 ]", "")],
            "MES": [("[^A-Za-z0-9 ]", ""), ("[.0]", "")],
            "REMUNERACAO_BASICA_BRUTA_BRL": [("[,]", ".")],
            "REMUNERACAO_BASICA_BRUTA_USD": [("[,]", ".")],
            "ABATE_TETO_BRL": [("[,]", ".")],
            "ABATE_TETO_USD": [("[,]", ".")],
            "GRATIFICACAO_NATALINA_BRUTA_BRL": [("[,]", ".")],
            "GRATIFICACAO_NATALINA_BRUTA_USD": [("[,]", ".")],
            "ABATE_TETO_GRATIFICACAO_NATALINA_BRL": [("[,]", ".")],
            "ABATE_TETO_GRATIFICACAO_NATALINA_USD": [("[,]", ".")],
            "FERIAS_BRL": [("[,]", ".")],
            "FERIAS_USD": [("[,]", ".")],
            "OUTRAS_REMUNERACOES_EVENTUAIS_BRL": [("[,]", ".")],
            "OUTRAS_REMUNERACOES_EVENTUAIS_USD": [("[,]", ".")],
            "IRRF_BRL": [("[,]", ".")],
            "IRRF_USD": [("[,]", ".")],
            "PSS_RPGS_BRL": [("[,]", ".")],
            "PSS_RPGS_USD": [("[,]", ".")],
            "DEMAIS_DEDUCOES_BRL": [("[,]", ".")],
            "DEMAIS_DEDUCOES_USD": [("[,]", ".")],
            "PENSAO_MILITAR_BRL": [("[,]", ".")],
            "PENSAO_MILITAR_USD": [("[,]", ".")],
            "FUNDO_SAUDE_BRL": [("[,]", ".")],
            "FUNDO_SAUDE_USD": [("[,]", ".")],
            "TAXA_OCUPACAO_IMOVEL_FUNCIONAL_BRL": [("[,]", ".")],
            "TAXA_OCUPACAO_IMOVEL_FUNCIONAL_USD": [("[,]", ".")],
            "REMUNERACAO_APOS_DEDUCOES_OBRIGATORIAS_BRL": [("[,]", ".")],
            "REMUNERACAO_APOS_DEDUCOES_OBRIGATORIAS_USD": [("[,]", ".")],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_CIVIL_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_CIVIL_USD": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_MILITAR_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_MILITAR_USD": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_PROGRAMA_DESLIGAMENTO_VOLUNTARIO_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_PROGRAMA_DESLIGAMENTO_VOLUNTARIO_USD": [
                ("[,]", ".")
            ],
            "TOTAL_VERBAS_INDENIZATORIAS_BRL": [("[,]", ".")],
            "TOTAL_VERBAS_INDENIZATORIAS_USD": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_reserva_militares(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ID_SERVIDOR_PORTAL": [("[^A-Za-z0-9 ]", "")],
            "NOME": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "MATRICULA": [("[^A-Za-z0-9 ]", "")],
            "COD_TIPO_APOSENTADORIA": [("[^A-Za-z0-9 ]", "")],
            "TIPO_APOSENTADORIA": [("[^A-Za-z0-9 ]", " ")],
            "DATA_APOSENTADORIA": [("/", "-")],
            "DESCRICAO_CARGO": [("[^A-Za-z0-9 ]", " ")],
            "COD_UORG_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "UORG_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORG_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "ORG_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORGSUP_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "ORGSUP_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_TIPO_VINCULO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_VINCULO": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_VINCULO": [("[^A-Za-z0-9 ]", "")],
            "REGIME_JURIDICO": [("[^A-Za-z0-9 ]", "")],
            "JORNADA_DE_TRABALHO": [("[^A-Za-z0-9 ]", "")],
            "DATA_INGRESSO_CARGOFUNCAO": [("/", "-")],
            "DATA_NOMEACAO_CARGOFUNCAO": [("/", "-")],
            "DATA_INGRESSO_ORGAO": [("/", "-")],
            "DOCUMENTO_INGRESSO_SERVICO_PUBLICO": [("[^A-Za-z0-9 ]", "")],
            "DATA_DIPLOMA_INGRESSO_SERVICO_PUBLICO": [("/", "-")],
            "DIPLOMA_INGRESSO_CARGOFUNCAO": [("[^A-Za-z0-9 ]", "")],
            "DIPLOMA_INGRESSO_ORGAO": [("[^A-Za-z0-9 ]", "")],
            "DIPLOMA_INGRESSO_SERVICOPUBLICO": [("[^A-Za-z0-9 ]", "")],
            "ANO": [("[^A-Za-z0-9 ]", "")],
            "MES": [("[^A-Za-z0-9 ]", ""), ("[.0]", "")],
            "REMUNERACAO_BASICA_BRUTA_BRL": [("[,]", ".")],
            "REMUNERACAO_BASICA_BRUTA_USD": [("[,]", ".")],
            "ABATE_TETO_BRL": [("[,]", ".")],
            "ABATE_TETO_USD": [("[,]", ".")],
            "GRATIFICACAO_NATALINA_BRUTA_BRL": [("[,]", ".")],
            "GRATIFICACAO_NATALINA_BRUTA_USD": [("[,]", ".")],
            "ABATE_TETO_GRATIFICACAO_NATALINA_BRL": [("[,]", ".")],
            "ABATE_TETO_GRATIFICACAO_NATALINA_USD": [("[,]", ".")],
            "FERIAS_BRL": [("[,]", ".")],
            "FERIAS_USD": [("[,]", ".")],
            "OUTRAS_REMUNERACOES_EVENTUAIS_BRL": [("[,]", ".")],
            "OUTRAS_REMUNERACOES_EVENTUAIS_USD": [("[,]", ".")],
            "IRRF_BRL": [("[,]", ".")],
            "IRRF_USD": [("[,]", ".")],
            "PSS_RPGS_BRL": [("[,]", ".")],
            "PSS_RPGS_USD": [("[,]", ".")],
            "DEMAIS_DEDUCOES_BRL": [("[,]", ".")],
            "DEMAIS_DEDUCOES_USD": [("[,]", ".")],
            "PENSAO_MILITAR_BRL": [("[,]", ".")],
            "PENSAO_MILITAR_USD": [("[,]", ".")],
            "FUNDO_SAUDE_BRL": [("[,]", ".")],
            "FUNDO_SAUDE_USD": [("[,]", ".")],
            "TAXA_OCUPACAO_IMOVEL_FUNCIONAL_BRL": [("[,]", ".")],
            "TAXA_OCUPACAO_IMOVEL_FUNCIONAL_USD": [("[,]", ".")],
            "REMUNERACAO_APOS_DEDUCOES_OBRIGATORIAS_BRL": [("[,]", ".")],
            "REMUNERACAO_APOS_DEDUCOES_OBRIGATORIAS_USD": [("[,]", ".")],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_CIVIL_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_CIVIL_USD": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_MILITAR_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_MILITAR_USD": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_PROGRAMA_DESLIGAMENTO_VOLUNTARIO_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_PROGRAMA_DESLIGAMENTO_VOLUNTARIO_USD": [
                ("[,]", ".")
            ],
            "TOTAL_VERBAS_INDENIZATORIAS_BRL": [("[,]", ".")],
            "TOTAL_VERBAS_INDENIZATORIAS_USD": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_servidores(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ID_SERVIDOR_PORTAL": [("[^A-Za-z0-9 ]", "")],
            "NOME": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "MATRICULA": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO_CARGO": [("[^A-Za-z0-9 ]", " ")],
            "CLASSE_CARGO": [("[^A-Za-z0-9 ]", "")],
            "REFERENCIA_CARGO": [("[^A-Za-z0-9 ]", "")],
            "PADRAO_CARGO": [("[^A-Za-z0-9 ]", "")],
            "NIVEL_CARGO": [("[^A-Za-z0-9 ]", "")],
            "SIGLA_FUNCAO": [("[^A-Za-z0-9 ]", "")],
            "NIVEL_FUNCAO": [("[^A-Za-z0-9 ]", "")],
            "FUNCAO": [("[^A-Za-z0-9 ]", " ")],
            "CODIGO_ATIVIDADE": [("[^A-Za-z0-9 ]", "")],
            "ATIVIDADE": [("[^A-Za-z0-9 ]", " ")],
            "OPCAO_PARCIAL": [("[^A-Za-z0-9 ]", "")],
            "COD_UORG_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "UORG_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORG_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "ORG_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORGSUP_LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "ORGSUP_LOTACAO": [("[^A-Za-z0-9 ]", " ")],
            "COD_UORG_EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "UORG_EXERCICIO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORG_EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "ORG_EXERCICIO": [("[^A-Za-z0-9 ]", " ")],
            "COD_ORGSUP_EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "ORGSUP_EXERCICIO": [("[^A-Za-z0-9 ]", " ")],
            "COD_TIPO_VINCULO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_VINCULO": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_VINCULO": [("[^A-Za-z0-9 ]", " ")],
            "DATA_INICIO_AFASTAMENTO": [("/", "-")],
            "DATA_TERMINO_AFASTAMENTO": [("/", "-")],
            "REGIME_JURIDICO": [("[^A-Za-z0-9 ]", "")],
            "JORNADA_DE_TRABALHO": [("[^A-Za-z0-9 ]", "")],
            "DATA_INGRESSO_CARGOFUNCAO": [("/", "-")],
            "DATA_NOMEACAO_CARGOFUNCAO": [("/", "-")],
            "DATA_INGRESSO_ORGAO": [("/", "-")],
            "DOCUMENTO_INGRESSO_SERVICO_PUBLICO": [("[^A-Za-z0-9 ]", "")],
            "DATA_DIPLOMA_INGRESSO_SERVICO_PUBLICO": [("/", "-")],
            "DIPLOMA_INGRESSO_CARGOFUNCAO": [("[^A-Za-z0-9 ]", " ")],
            "DIPLOMA_INGRESSO_ORGAO": [("[^A-Za-z0-9 ]", " ")],
            "DIPLOMA_INGRESSO_SERVICOPUBLICO": [("[^A-Za-z0-9 ]", " ")],
            "UF_EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "ANO": [("[^A-Za-z0-9 ]", "")],
            "MES": [("[^A-Za-z0-9 ]", ""), ("[.0]", "")],
            "REMUNERACAO_BASICA_BRUTA_BRL": [("[,]", ".")],
            "REMUNERACAO_BASICA_BRUTA_USD": [("[,]", ".")],
            "ABATE_TETO_BRL": [("[,]", ".")],
            "ABATE_TETO_USD": [("[,]", ".")],
            "GRATIFICACAO_NATALINA_BRUTA_BRL": [("[,]", ".")],
            "GRATIFICACAO_NATALINA_BRUTA_USD": [("[,]", ".")],
            "ABATE_TETO_GRATIFICACAO_NATALINA_BRL": [("[,]", ".")],
            "ABATE_TETO_GRATIFICACAO_NATALINA_USD": [("[,]", ".")],
            "FERIAS_BRL": [("[,]", ".")],
            "FERIAS_USD": [("[,]", ".")],
            "OUTRAS_REMUNERACOES_EVENTUAIS_BRL": [("[,]", ".")],
            "OUTRAS_REMUNERACOES_EVENTUAIS_USD": [("[,]", ".")],
            "IRRF_BRL": [("[,]", ".")],
            "IRRF_USD": [("[,]", ".")],
            "PSS_RPGS_BRL": [("[,]", ".")],
            "PSS_RPGS_USD": [("[,]", ".")],
            "DEMAIS_DEDUCOES_BRL": [("[,]", ".")],
            "DEMAIS_DEDUCOES_USD": [("[,]", ".")],
            "PENSAO_MILITAR_BRL": [("[,]", ".")],
            "PENSAO_MILITAR_USD": [("[,]", ".")],
            "FUNDO_SAUDE_BRL": [("[,]", ".")],
            "FUNDO_SAUDE_USD": [("[,]", ".")],
            "TAXA_OCUPACAO_IMOVEL_FUNCIONAL_BRL": [("[,]", ".")],
            "TAXA_OCUPACAO_IMOVEL_FUNCIONAL_USD": [("[,]", ".")],
            "REMUNERACAO_APOS_DEDUCOES_OBRIGATORIAS_BRL": [("[,]", ".")],
            "REMUNERACAO_APOS_DEDUCOES_OBRIGATORIAS_USD": [("[,]", ".")],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_CIVIL_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_CIVIL_USD": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_MILITAR_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_REGISTRADAS_SISTEMAS_PESSOAL_MILITAR_USD": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_PROGRAMA_DESLIGAMENTO_VOLUNTARIO_BRL": [
                ("[,]", ".")
            ],
            "VERBAS_INDENIZATORIAS_PROGRAMA_DESLIGAMENTO_VOLUNTARIO_USD": [
                ("[,]", ".")
            ],
            "TOTAL_VERBAS_INDENIZATORIAS_BRL": [("[,]", ".")],
            "TOTAL_VERBAS_INDENIZATORIAS_USD": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_servidores_sp(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "NOME": [("[^A-Za-z0-9 ]", " ")],
            "CARGO": [("[^A-Za-z0-9 ]", " ")],
            "ORGAO": [("[^A-Za-z0-9 ]", " ")],
            "REMUNERACAO_DO_MES": [("[,]", ".")],
            "FERIAS_E_13_SALARIO": [("[,]", ".")],
            "PAGAMENTOS_EVENTUAIS": [("[,]", ".")],
            "LICENCA_PREMIO_INDENIZADA": [("[,]", ".")],
            "ABONO_PERMANENCIA_E_OUTRAS_INDENIZACOES": [("[,]", ".")],
            "REDUTOR_SALARIAL": [("[,]", ".")],
            "TOTAL_LIQUIDO": [("[,]", ".")],
            "GRUPO": [("[^A-Za-z0-9 ]", " ")],
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = dataframe.withColumn(
            "MES_REFERENCIA",
            concat(
                substring(col("ARQUIVO"), 0, 4),
                lit("-"),
                substring(col("ARQUIVO"), 5, 2),
                lit("-01"),
            ),
        )

        return dataframe

    def columns_servidores_mg(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "MASP": [("[^A-Za-z0-9 ]", " ")],
            "NOME": [("[^A-Za-z0-9 ]", " ")],
            "DESCSITSER": [("[^A-Za-z0-9 ]", " ")],
            "NMEFET": [("[^A-Za-z0-9 ]", " ")],
            "TEM_APOST": [("[^A-Za-z0-9 ]", " ")],
            "DESCCOMI": [("[^A-Za-z0-9 ]", " ")],
            "DESCINST": [("[^A-Za-z0-9 ]", " ")],
            "DESCUNID": [("[^A-Za-z0-9 ]", " ")],
            "CARGA_HORA": [("[,]", ".")],
            "REMUNER": [("[,]", ".")],
            "TETO": [("[,]", ".")],
            "JUDIC": [("[,]", ".")],
            "FERIAS": [("[,]", ".")],
            "DECTER": [("[,]", ".")],
            "PREMIO": [("[,]", ".")],
            "FERIASPREM": [("[,]", ".")],
            "JETONS": [("[,]", ".")],
            "EVENTUAL": [("[,]", ".")],
            "IR": [("[,]", ".")],
            "PREV": [("[,]", ".")],
            "REM_POS": [("[,]", ".")],
            "BDMG": [("[,]", ".")],
            "CEMIG": [("[,]", ".")],
            "CODEMIG": [("[,]", ".")],
            "COHAB": [("[,]", ".")],
            "COPASA": [("[,]", ".")],
            "EMATER": [("[,]", ".")],
            "EPAMIG": [("[,]", ".")],
            "FUNPEMG": [("[,]", ".")],
            "GASMIG": [("[,]", ".")],
            "MGI": [("[,]", ".")],
            "MGS": [("[,]", ".")],
            "PRODEMGE": [("[,]", ".")],
            "PROMINAS": [("[,]", ".")],
            "EMIP": [("[,]", ".")],
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = dataframe.withColumn(
            "MES_REFERENCIA",
            concat(
                substring(col("ARQUIVO"), 0, 4),
                lit("-"),
                substring(col("ARQUIVO"), 5, 2),
                lit("-01"),
            ),
        )

        return dataframe

    def columns_servidores_es(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ORGAO": [
                (
                    "[^\\w\\d\u00C0-\u017F]+|(?<=\\d)[^\\w\\s\u00C0-\u017F]+|(?=[^\\w\\s\u00C0-\u017F]*\\d)+|_",
                    " ",
                )
            ],
            "NUM_FUNC": [("[^A-Za-z0-9 ]", "")],
            "NUM_VINC": [("[^A-Za-z0-9 ]", "")],
            "NOME": [
                (
                    "[^\\w\\d\u00C0-\u017F]+|(?<=\\d)[^\\w\\s\u00C0-\u017F]+|(?=[^\\w\\s\u00C0-\u017F]*\\d)+|_",
                    " ",
                )
            ],
            "NUM_PENS": [("[^A-Za-z0-9 ]", "")],
            "NOME_PENSIONISTA": [
                (
                    "[^\\w\\d\u00C0-\u017F]+|(?<=\\d)[^\\w\\s\u00C0-\u017F]+|(?=[^\\w\\s\u00C0-\u017F]*\\d)+|_",
                    " ",
                )
            ],
            "CENTRO_CUSTO": [
                (
                    "[^\\w\\d\u00C0-\u017F]+|(?<=\\d)[^\\w\\s\u00C0-\u017F]+|(?=[^\\w\\s\u00C0-\u017F]*\\d)+|_",
                    " ",
                )
            ],
            "MES_FOLHA": [("[^A-Za-z0-9 ]", "")],
            "MES_COMPETENCIA": [("[^A-Za-z0-9 ]", "")],
            "FUNCAO": [
                (
                    "[^\\w\\d\u00C0-\u017F]+|(?<=\\d)[^\\w\\s\u00C0-\u017F]+|(?=[^\\w\\s\u00C0-\u017F]*\\d)+|_",
                    " ",
                )
            ],
            "CARGO": [
                (
                    "[^\\w\\d\u00C0-\u017F]+|(?<=\\d)[^\\w\\s\u00C0-\u017F]+|(?=[^\\w\\s\u00C0-\u017F]*\\d)+|_",
                    " ",
                )
            ],
            "COD_CARGO": [("[^A-Za-z0-9 ]", "")],
            "COD_FUNCAO": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_FOLHA": [
                (
                    "[^\\w\\d\u00C0-\u017F]+|(?<=\\d)[^\\w\\s\u00C0-\u017F]+|(?=[^\\w\\s\u00C0-\u017F]*\\d)+|_",
                    " ",
                )
            ],
            "COD_RUBRICA": [("[^A-Za-z0-9 ]", "")],
            "RUBRICA": [
                (
                    "[^\\w\\d\u00C0-\u017F]+|(?<=\\d)[^\\w\\s\u00C0-\u017F]+|(?=[^\\w\\s\u00C0-\u017F]*\\d)+|_",
                    " ",
                )
            ],
            "VANTAGEM_DESCONTO": [
                (
                    "[^\\w\\d\u00C0-\u017F]+|(?<=\\d)[^\\w\\s\u00C0-\u017F]+|(?=[^\\w\\s\u00C0-\u017F]*\\d)+|_",
                    " ",
                )
            ],
            "TIPO_FUNCAO": [
                (
                    "[^\\w\\d\u00C0-\u017F]+|(?<=\\d)[^\\w\\s\u00C0-\u017F]+|(?=[^\\w\\s\u00C0-\u017F]*\\d)+|_",
                    " ",
                )
            ],
            "VALOR": [("[^A-Za-z][0,9][.]{2}", "")],
            "ID": [("[^A-Za-z0-9 ]", "")],
        }

        date_columns = ["MES_FOLHA", "MES_COMPETENCIA"]

        start1 = 3
        len1 = 4

        start2 = 0
        len2 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=df_for_clean,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2
        )
        return dataframe

    def columns_servidores_sc(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "NOME": [("[^A-Za-z0-9 ]", " ")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "CARGO": [("[^A-Za-z0-9 ]", "")],
            "ORGAO_EXERCICIO": [("[^A-Za-z0-9 ]", " ")],
            "UNIDADE_EXERCICIO": [("[^A-Za-z0-9 ]", " ")],
            "SITUACAO": [("[^A-Za-z0-9 ]", "")],
            "VALOR_BRUTO": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    # FORNECEDORES GOVERNO ===================================================
    def columns_fornecedores_compras(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "NUMERO_DO_CONTRATO": [("[^A-Za-z0-9 ]", "")],
            "OBJETO": [("[^A-Za-z0-9 ]", "")],
            "FUNDAMENTO_LEGAL": [("[^A-Za-z0-9 ]", "")],
            "MODALIDADE_COMPRA": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_CONTRATO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_ORGAO_SUPERIOR": [("[^A-Za-z0-9 ]", "")],
            "NOME_ORGAO_SUPERIOR": [("[^A-Za-z0-9 ]", " ")],
            "CODIGO_ORGAO": [("[^A-Za-z0-9 ]", "")],
            "NOME_ORGAO": [("[^A-Za-z0-9 ]", " ")],
            "CODIGO_UG": [("[^A-Za-z0-9 ]", "")],
            "NOME_UG": [("[^A-Za-z0-9 ]", " ")],
            "DATA_ASSINATURA_CONTRATO": [("[^A-Za-z0-9 ]", "")],
            "DATA_PUBLICACAO_DOU": [("[^A-Za-z0-9 ]", "")],
            "DATA_INICIO_VIGENCIA": [("[^A-Za-z0-9 ]", "")],
            "DATA_FIM_VIGENCIA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_CONTRATADO": [("[^A-Za-z0-9 ]", "")],
            "NOME_CONTRATADO": [("[^A-Za-z0-9 ]", "")],
            "VALOR_INICIAL_COMPRA": [("[,]", "."), ("\\.?0+$", "")],
            "VALOR_FINAL_COMPRA": [("[,]", "."), ("\\.?0+$", "")],
            "NUMERO_LICITACAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_UG_LICITACAO": [("[^A-Za-z0-9 ]", "")],
            "NOME_UG_LICITACAO": [("[^A-Za-z0-9 ]", " ")],
            "CODIGO_MODALIDADE_COMPRA_LICITACAO": [("[^A-Za-z0-9 ]", "")],
            "MODALIDADE_COMPRA_LICITACAO": [("[^A-Za-z0-9 ]", "")],
        }

        date_columns = [
            "DATA_ASSINATURA_CONTRATO",
            "DATA_PUBLICACAO_DOU",
            "DATA_INICIO_VIGENCIA",
            "DATA_FIM_VIGENCIA",
        ]

        start1 = 5
        len1 = 4

        start2 = 3
        len2 = 2

        start3 = 1
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    def columns_fornecedores_item_compra(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CODIGO_ORGAO": [("[^A-Za-z0-9 ]", "")],
            "NOME_ORGAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_UG": [("[^A-Za-z0-9 ]", "")],
            "NOME_UG": [("[^A-Za-z0-9 ]", " ")],
            "NUMERO_CONTRATO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_ITEM_COMPRA": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO_ITEM_COMPRA": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO_COMPLEMENTAR_ITEM_COMPRA": [("[^A-Za-z0-9 ]", "")],
            "QUANTIDADE_ITEM": [("[^A-Za-z0-9 ]", "")],
            "VALOR_ITEM": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_fornecedores_termo_aditivo(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "NUMERO_CONTRATO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_ORGAO_SUPERIOR": [("[^A-Za-z0-9 ]", "")],
            "NOME_ORGAO_SUPERIOR": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_ORGAO": [("[^A-Za-z0-9 ]", "")],
            "NOME_ORGAO": [("[^A-Za-z0-9 ]", " ")],
            "CODIGO_UG": [("[^A-Za-z0-9 ]", "")],
            "NOME_UG": [("[^A-Za-z0-9 ]", " ")],
            "NUMERO_TERMO_ADITIVO": [("[^A-Za-z0-9 ]", "")],
            "DATA_PUBLICACAO": [("[^A-Za-z0-9 ]", "")],
            "OBJETO": [("[^A-Za-z0-9 ]", "")],
        }

        date_columns = ["DATA_PUBLICACAO"]

        start1 = 5
        len1 = 4

        start2 = 3
        len2 = 2

        start3 = 1
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    # CNPJ ===================================================================
    def columns_cnpj_empresas(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CNPJ_BASICO": [("[^A-Za-z0-9 ]", "")],
            "RAZAO_SOCIAL": [("[^A-Za-z0-9 ]", "")],
            "NATUREZA_JURIDICA": [("[^A-Za-z0-9 ]", "")],
            "QUALIFICACAO_DO_RESPONSAVEL": [("[^A-Za-z0-9 ]", "")],
            "CAPITAL_SOCIAL_DA_EMPRESA": [("[,]", ".")],
            "PORTE_DA_EMPRESA": [("[^A-Za-z0-9 ]", "")],
            "ENTE_FEDERATIVO_RESPONSAVEL": [("[^A-Za-z0-9 ]", "")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_cnpj_estabelecimentos(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CNPJ_BASICO": [("[^A-Za-z0-9 ]", "")],
            "CNPJ_ORDEM": [("[^A-Za-z0-9 ]", "")],
            "CNPJ_DV": [("[^A-Za-z0-9 ]", "")],
            "IDENTIFICADOR": [("[^A-Za-z0-9 ]", "")],
            "NOME_FANTASIA": [("[^A-Za-z0-9 ]", " ")],
            "SITUACAO_CADASTRAL": [("[^A-Za-z0-9 ]", "")],
            "DATA_SITUACAO_CADASTRAL": [("[^A-Za-z0-9 ]", "")],
            "MOTIVO_SITUACAO_CADASTRAL": [("[^A-Za-z0-9 ]", "")],
            "NOME_DA_CIDADE_NO_EXTERIOR": [("[^A-Za-z0-9 ]", "")],
            "PAIS": [("[^A-Za-z0-9 ]", "")],
            "DATA_INICIO_ATIVIDADE": [("[^A-Za-z0-9 ]", "")],
            "CNAE_FISCAL_PRINCIPAL": [("[^A-Za-z0-9 ]", "")],
            "CNAE_FISCAL_SECUNDARIA": [("[^A-Za-z0-9 ]", "")],
            "TIPO_DE_LOGRADOURO": [("[^A-Za-z0-9 ]", "")],
            "LOGRADOURO": [("[^A-Za-z0-9 ]", " ")],
            "NUMERO": [("[^A-Za-z0-9 ]", " ")],
            "COMPLEMENTO": [("[^A-Za-z0-9 ]", " ")],
            "BAIRRO": [("[^A-Za-z0-9 ]", " ")],
            "CEP": [("[^A-Za-z0-9 ]", "")],
            "UF": [("[^A-Za-z0-9 ]", "")],
            "MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "DDD_1": [("[^A-Za-z0-9 ]", "")],
            "TELEFONE_1": [("[^A-Za-z0-9 ]", "")],
            "DDD_2": [("[^A-Za-z0-9 ]", "")],
            "TELEFONE_2": [("[^A-Za-z0-9 ]", "")],
            "DDD_FAX": [("[^A-Za-z0-9 ]", "")],
            "FAX": [("[^A-Za-z0-9 ]", "")],
            "CORREIO_ELETRONICO": [("", "")],
            "SITUACAO_ESPECIAL": [("[^A-Za-z0-9 ]", "")],
            "DATA_SITUACAO_ESPECIAL": [("[^A-Za-z0-9 ]", "")],
        }

        date_columns = [
            "DATA_SITUACAO_CADASTRAL",
            "DATA_INICIO_ATIVIDADE",
            "DATA_SITUACAO_ESPECIAL",
        ]

        start1 = 1
        len1 = 4

        start2 = 5
        len2 = 2

        start3 = 7
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    def columns_cnpj_simples(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CNPJ_BASICO": [("[^A-Za-z0-9 ]", "")],
            "OPCAO_PELO_SIMPLES": [("[^A-Za-z0-9 ]", "")],
            "DATA_DE_OPCAO_PELO_SIMPLES": [("[^A-Za-z0-9 ]", "")],
            "DATA_DE_EXCLUSAO_DO_SIMPLES": [("[^A-Za-z0-9 ]", "")],
            "OPCAO_PELO_MEI": [("[,]", ".")],
            "DATA_DE_OPCAO_PELO_MEI": [("[^A-Za-z0-9 ]", "")],
            "DATA_DE_EXCLUSAO_DO_MEI": [("[^A-Za-z0-9 ]", "")],
        }

        date_columns = [
            "DATA_DE_OPCAO_PELO_SIMPLES",
            "DATA_DE_EXCLUSAO_DO_SIMPLES",
            "DATA_DE_OPCAO_PELO_MEI",
            "DATA_DE_EXCLUSAO_DO_MEI",
        ]

        start1 = 1
        len1 = 4

        start2 = 5
        len2 = 2

        start3 = 7
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        for column in date_columns:
            dataframe = dataframe.withColumn(
                column, regexp_replace(col(column), "0--", "")
            )

        return dataframe

    def columns_cnpj_socios(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CNPJ_BASICO": [("[^A-Za-z0-9 ]", "")],
            "IDENTIFICADOR_DE_SOCIO": [("[^A-Za-z0-9 ]", "")],
            "NOME_DO_SOCIO_OU_RAZAO_SOCIAL": [("[^A-Za-z0-9 ]", " ")],
            "CPF_OU_CNPJ_DO_SOCIO": [("[^A-Za-z0-9 ]", "")],
            "QUALIFICACAO_DO_SOCIO": [("[^A-Za-z0-9 ]", "")],
            "DATA_DE_ENTRADA_SOCIEDADE": [("[^A-Za-z0-9 ]", "")],
            "PAIS": [("[^A-Za-z0-9 ]", "")],
            "REPRESENTANTE_LEGAL": [("[^A-Za-z0-9 ]", "")],
            "NOME_DO_REPRESENTANTE_LEGAL": [("[^A-Za-z0-9 ]", "")],
            "QUALIFICACAO_DO_REPRESENTANTE_LEGAL": [("[^A-Za-z0-9 ]", "")],
            "FAIXA_ETARIA": [("[^A-Za-z0-9 ]", "")],
        }

        date_columns = ["DATA_DE_ENTRADA_SOCIEDADE"]

        start1 = 1
        len1 = 4

        start2 = 5
        len2 = 2

        start3 = 7
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            df_for_clean, date_columns, start1, len1, start2, len2, start3, len3
        )

        return dataframe

    def columns_cnpj_cnaes(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CODIGO": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO": [("[^A-Za-z0-9 ]", " ")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_cnpj_paises(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CODIGO": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO": [("[^A-Za-z0-9 ]", " ")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_cnpj_natureza_juridica(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CODIGO": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO": [("[^A-Za-z0-9 ]", " ")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_cnpj_qualific_socios(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CODIGO": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO": [("[^A-Za-z0-9 ]", " ")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_cnpj_municipios(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CODIGO": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO": [("[^A-Za-z0-9 ]", " ")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_cnpj_motivos(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CODIGO": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO": [("[^A-Za-z0-9 ]", " ")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_cnpj_imunes(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ANO": [("[^A-Za-z0-9 ]", "")],
            "CNPJ": [("[^A-Za-z0-9 ]", "")],
            "CNPJ_DA_SCP": [("[^A-Za-z0-9 ]", "")],
            "FORMA_DE_TRIBUTACAO": [("[^A-Za-z0-9 ]", "")],
            "QUANTIDADE_DE_ESCRITURACOES": [("[^A-Za-z0-9 ]", "")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_cnpj_lucro_arbitrado(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ANO": [("[^A-Za-z0-9 ]", "")],
            "CNPJ": [("[^A-Za-z0-9 ]", "")],
            "CNPJ_DA_SCP": [("[^A-Za-z0-9 ]", "")],
            "FORMA_DE_TRIBUTACAO": [("[^A-Za-z0-9 ]", "")],
            "QUANTIDADE_DE_ESCRITURACOES": [("[^A-Za-z0-9 ]", "")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_cnpj_lucro_presumido(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ANO": [("[^A-Za-z0-9 ]", "")],
            "CNPJ": [("[^A-Za-z0-9 ]", "")],
            "CNPJ_DA_SCP": [("[^A-Za-z0-9 ]", "")],
            "FORMA_DE_TRIBUTACAO": [("[^A-Za-z0-9 ]", "")],
            "QUANTIDADE_DE_ESCRITURACOES": [("[^A-Za-z0-9 ]", "")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_cnpj_lucro_real(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ANO": [("[^A-Za-z0-9 ]", "")],
            "CNPJ": [("[^A-Za-z0-9 ]", "")],
            "CNPJ_DA_SCP": [("[^A-Za-z0-9 ]", "")],
            "FORMA_DE_TRIBUTACAO": [("[^A-Za-z0-9 ]", "")],
            "QUANTIDADE_DE_ESCRITURACOES": [("[^A-Za-z0-9 ]", "")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    # DÉBITOS TRABALHISTAS ===================================================
    def columns_debitos_prev(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CPF_CNPJ": [("[^0-9 ]", "")],
            "TIPO_PESSOA": [("[^A-Za-z0-9 ]", "")],
            "TIPO_DEVEDOR": [("[^A-Za-z0-9 ]", "")],
            "NOME_DEVEDOR": [("[^A-Za-z0-9 ]", "")],
            "UF_DEVEDOR": [("[^A-Za-z0-9 ]", "")],
            "UNIDADE_RESPONSAVEL": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_INSCRICAO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_SITUACAO_INSCRICAO": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_INSCRICAO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_CREDITO": [("[^A-Za-z0-9 ]", "")],
            "DATA_INSCRICAO": [("/", "-")],
            "INDICADOR_AJUIZADO": [("[^A-Za-z0-9 ]", "")],
            "VALOR_CONSOLIDADO": [("[,]", ",")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_debitos_nao_prev(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CPF_CNPJ": [("[^0-9 ]", "")],
            "TIPO_PESSOA": [("[^A-Za-z0-9 ]", "")],
            "TIPO_DEVEDOR": [("[^A-Za-z0-9 ]", "")],
            "NOME_DEVEDOR": [("[^A-Za-z0-9 ]", "")],
            "UF_DEVEDOR": [("[^A-Za-z0-9 ]", "")],
            "UNIDADE_RESPONSAVEL": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_INSCRICAO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_SITUACAO_INSCRICAO": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_INSCRICAO": [("[^A-Za-z0-9 ]", "")],
            "RECEITA_PRINCIPAL": [("[^A-Za-z0-9 ]", "")],
            "DATA_INSCRICAO": [("/", "-")],
            "INDICADOR_AJUIZADO": [("[^A-Za-z0-9 ]", "")],
            "VALOR_CONSOLIDADO": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_debitos_fgts(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CPF_CNPJ": [("[^0-9 ]", "")],
            "TIPO_PESSOA": [("[^A-Za-z0-9 ]", "")],
            "TIPO_DEVEDOR": [("[^A-Za-z0-9 ]", "")],
            "NOME_DEVEDOR": [("[^A-Za-z0-9 ]", "")],
            "UF_DEVEDOR": [("[^A-Za-z0-9 ]", "")],
            "UNIDADE_RESPONSAVEL": [("[^A-Za-z0-9 ]", "")],
            "ENTIDADE_RESPONSAVEL": [("[^A-Za-z0-9 ]", "")],
            "UNIDADE_INSCRICAO": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_INSCRICAO": [("[^A-Za-z0-9 ]", "")],
            "TIPO_SITUACAO_INSCRICAO": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO_INSCRICAO": [("[^A-Za-z0-9 ]", "")],
            "RECEITA_PRINCIPAL": [("[^A-Za-z0-9 ]", "")],
            "DATA_INSCRICAO": [("/", "-")],
            "INDICADOR_AJUIZADO": [("[^A-Za-z0-9 ]", "")],
            "VALOR_CONSOLIDADO": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_servidores_pr(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "COD_VINCULO": [("[^A-Za-z0-9 ]", "")],
            "NOME": [("[^A-Za-z0-9 ]", "")],
            "SIGLA": [("[^A-Za-z0-9 ]", "")],
            "INSTITUICAO": [("[^A-Za-z0-9 ]", "")],
            "LOTACAO": [("[^A-Za-z0-9 ]", "")],
            "MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "CARGO": [("[^A-Za-z0-9 ]", " ")],
            "DT_INICIO": [("/", "-")],
            "DT_FIM": [("/", "-")],
            "REGIME": [("[^A-Za-z0-9 ]", "")],
            "QUADRO_FUNCIONAL": [("[^A-Za-z0-9 ]", "")],
            "QUADRO_FUNCIONAL_DESC": [("[^A-Za-z0-9 ]", "")],
            "TIPO_CARGO": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO": [("[^A-Za-z0-9 ]", "")],
            "ULT_REMU_BRUTA": [("[,]", ".")],
            "GENERO": [("[^A-Za-z0-9 ]", "")],
            "ANO_NASC": [("\.0*$", "")],
            "ATUALIZADO": [("/", "-")],
        }
        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        return dataframe

    def columns_novo_bolsa_familia(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "MES_COMPETENCIA": [("[^A-Za-z0-9 ]", "")],
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "UF": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MUNICIPIO_SIAFI": [("[^A-Za-z0-9 ]", "")],
            "NOME_MUNICIPIO": [("[^A-Za-z0-9 ]", "")],
            "CPF_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NIS_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "NOME_FAVORECIDO": [("[^A-Za-z0-9 ]", "")],
            "VALOR_PARCELA": [("[^A-Za-z][0,9][.]{2}", ""), ("[,]", ".")],
        }

        date_columns_1 = ["MES_COMPETENCIA", "MES_REFERENCIA"]

        start1 = 0
        len1 = 4

        start2 = 5
        len2 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=df_for_clean,
            columns=date_columns_1,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2,
        )

        return dataframe

    def columns_servidores_df_orgao(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "NOME": [("[^A-Za-z0-9 ]", "")], 
            "MATRICULA": [("[^A-Za-z0-9 ]", "")], 
            "ORGAO": [("[^A-Za-z0-9 ]", "")], 
            "CARGO": [("[^A-Za-z0-9 ]", "")], 
            "FUNCAO": [("[^A-Za-z0-9 ]", "")], 
            "SIMBOLO_DA_FUNCAO": [("[^A-Za-z0-9 ]", "")], 
            "SITUACAO_FUNCIONAL_GERAL": [("[^A-Za-z0-9 ]", "")], 
            "CARREIRA": [("[^A-Za-z0-9 ]", "")], 
            "LOTACAO": [("[^A-Za-z0-9 ]", "")], 
            "DATA_ADMISSAO": [("[^A-Za-z0-9 ]", "")], 
            "DATA_ADMISSAO_FORMATO_NUMERICO": [("[^A-Za-z0-9 ]", "")], 
            "SITUACAO": [("[^A-Za-z0-9 ]", "")], 
            "TIPO_VINCULO": [("[^A-Za-z0-9 ]", "")], 
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")], 
            "SITUACAO_FUNCIONAL_ESPECIFICA": [("[^A-Za-z0-9 ]", "")]
        }

        date_columns_1 = ["DATA_ADMISSAO"]

        start1 = 5
        len1 = 4

        start2 = 3
        len2 = 2

        start3 = 1
        len3 = 2

        date_columns_2 = ["DATA_ADMISSAO_FORMATO_NUMERICO"]

        start4 = 1
        len4 = 4

        start5 = 5
        len5 = 2

        start6 = 7
        len6 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=dataframe,
            columns=date_columns_1,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2,
            start3=start3,
            len3=len3,
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=dataframe,
            columns=date_columns_2,
            start1=start4,
            len1=len4,
            start2=start5,
            len2=len5,
            start3=start6,
            len3=len6,
        )
        return dataframe

    def columns_servidores_df_receita(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "AA_EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "CO_UG": [("[^A-Za-z0-9 ]", "")],
            "CO_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "CO_CONTA_CONTABIL": [("[^A-Za-z0-9 ]", "")],
            "CO_FONTE_RECURSO": [("[^A-Za-z0-9 ]", "")],
            "CO_NATUREZA_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "CO_CATEGORIA_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "CO_FONTE_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "CO_SUB_FONTE_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "CO_RUBRICA_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "CO_ALINEA_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "CO_SUB_ALINEA_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "CO_TIPO_FONTE": [("[^A-Za-z0-9 ]", "")],
            "NO_UG": [("[^A-Za-z0-9 ]", "")],
            "NO_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "NO_CONTA_CONTABIL": [("[^A-Za-z0-9 ]", "")],
            "NO_FONTE_RECURSO": [("[^A-Za-z0-9 ]", "")],
            "NO_NATUREZA_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "NO_CATEGORIA_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "NO_FONTE_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "NO_SUB_FONTE_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "NO_RUBRICA_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "NO_ALINEA_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "NO_SUB_ALINEA_RECEITA": [("[^A-Za-z0-9 ]", "")],
            "NO_TIPO_FONTE": [("[^A-Za-z0-9 ]", "")],
            "VR_RECEITA": [("[,]", "."), ("^\.00$", "0")],
            "VR_RECEITA_REALIZADA": [("[,]", "."), ("^\.00$", "0")],
            "VR_RECEITA_PREVISTA": [("[,]", "."), ("^\.00$", "0")],
            "IN_MES": [("[^A-Za-z0-9 ]", "")],
            "DT_CARGA": [("[^A-Za-z0-9 ]", "")]
        }

        date_columns_1 = ["DT_CARGA"]

        start1 = 1
        len1 = 4

        start2 = 5
        len2 = 2

        start3 = 7
        len3 = 2
        
        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        dataframe = self.formatting.format_date_columns(
            dataframe=dataframe,
            columns=date_columns_1,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2,
            start3=start3,
            len3=len3,
        )

        return dataframe

    def columns_servidores_df_remuneracao(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "NOME": [("[^A-Za-z0-9 ]", "")],
            "CPF": [("[^A-Za-z0-9 ]", "")],
            "ORGAO": [("[^A-Za-z0-9 ]", "")],
            "CARGO": [("[^A-Za-z0-9 ]", "")],
            "FUNCAO": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO": [("[^A-Za-z0-9 ]", "")],
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "ANO_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_DO_ORGAO": [("[^A-Za-z0-9 ]", "")],
            "MATRICULA": [("[^A-Za-z0-9 ]", "")],
            "REMUNERACAO_BASICA": [("[,]", "."), ("^\.00$", "0")],
            "BENEFICIOS": [("[,]", "."), ("^\.00$", "0")],
            "VALOR_DA_FUNCAO": [("[,]", "."), ("^\.00$", "0")],
            "COMISSAO_CONSELHEIRO": [("[,]", "."), ("^\.00$", "0")],
            "HORA_EXTRA": [("[,]", "."), ("^\.00$", "0")],
            "VERBAS_EVENTUAIS": [("[,]", "."), ("^\.00$", "0")],
            "VERBAS_JUDICIAIS": [("[,]", "."), ("^\.00$", "0")],
            "DESCONTOS_A_MAIOR": [("[,]", "."), ("^\.00$", "0")],
            "LICENCA_PREMIO": [("[,]", "."), ("^\.00$", "0")],
            "IRRF": [("[,]", "."), ("^\.00$", "0")],
            "SEG_SOCIAL": [("[,]", "."), ("^\.00$", "0")],
            "TETO_REDUTOR": [("[,]", "."), ("^\.00$", "0")],
            "OUTROS_RECEBIMENTOS": [("[,]", "."), ("^\.00$", "0")],
            "OUTROS_DESCONTOS_OBRIGATORIOS": [("[,]", "."), ("^\.00$", "0")],
            "PAGAMENTOS_A_MAIOR": [("[,]", "."), ("^\.00$", "0")],
            "BRUTO": [("[,]", "."), ("^\.00$", "0")],
            "LIQUIDO": [("[,]", "."), ("^\.00$", "0")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_servidores_df_remuneracao_det(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CODIGO_DO_ORGAO": [("[^A-Za-z0-9 ]", "")],
            "MATRICULA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_RUBRICA": [("[^A-Za-z0-9 ]", "")],
            "NATUREZA": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO_RUBRICA": [("[^A-Za-z0-9 ]", "")],
            "TIPO_RUBRICA": [("[^A-Za-z0-9 ]", "")],
            "ANO_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "MES_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "VALOR": [("[,]", "."), ("^\.00$", "0")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_servidores_df_licitacao(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CODIGO_COMPRASNET": [("[^A-Za-z0-9 ]", "")],
            "NUMERO_LICITACAO": [("[^A-Za-z0-9 ]", "")],
            "EDITAL": [("[^A-Za-z0-9 ]", "")],
            "MODALIDADE": [("[^A-Za-z0-9 ]", "")],
            "ORGAO": [("[^A-Za-z0-9 ]", "")],
            "TIPO": [("[^A-Za-z0-9 ]", "")],
            "DATA_1": [("[^A-Za-z0-9 ]", "")],
            "DATA_2": [("[^A-Za-z0-9 ]", "")],
            "DATA_3": [("[^A-Za-z0-9 ]", "")],
            "SITUACAO": [("[^A-Za-z0-9 ]", "")],
            "TOTAL_PROPOSTO": [("[,]", ".")],
            "TOTAL_HOMOLOGADO": [("[,]", ".")]
        }
        date_columns = ["DATA_1", "DATA_2", "DATA_3"]

        start1 = 1
        len1 = 4

        start2 = 5
        len2 = 2

        start3 = 7
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )
        dataframe = self.formatting.format_date_columns(
            dataframe=dataframe,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2,
            start3=start3,
            len3=len3,
        )

        return dataframe

    def columns_servidores_df_ord_banc_cancel(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CD_UG": [("[^A-Za-z0-9 ]", "")],
            "CD_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "NR_NE": [("[^A-Za-z0-9 ]", "")],
            "NR_ANO_NE": [("[^A-Za-z0-9 ]", "")],
            "VL_LANCAMENTO": [("[,]", ".")],
            "CD_UG_LIQ": [("[^A-Za-z0-9 ]", "")],
            "CD_GESTAO_LIQ": [("[^A-Za-z0-9 ]", "")],
            "NR_OB": [("[^A-Za-z0-9 ]", "")],
            "NR_ANO_OB": [("[^A-Za-z0-9 ]", "")],
            "NR_OC": [("[^A-Za-z0-9 ]", "")],
            "NR_ANO_OC": [("[^A-Za-z0-9 ]", "")],
            "NR_DOC_REF": [("[^A-Za-z0-9 ]", "")],
            "NR_ANO_DOC_REF": [("[^A-Za-z0-9 ]", "")],
            "TP_DOC_REF": [("[^A-Za-z0-9 ]", "")],
            "DT_EMISSAO": [("/", "-")],
            "TX_OBSERVACAO": [("[^A-Za-z0-9 ]", "")],
            "VL_PAGO_EXERCICIO": [("[,]", ".")],
            "VL_PAGO_RPNP": [("[,]", ".")],
            "VL_PAGO_RPP": [("[,]", ".")],
            "VL_A_PAGAR_PAGO_RET": [("[,]", ".")],
            "VL_PAGO_RET": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_servidores_df_desp_nt_lanc_emp(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CD_UG": [("[^A-Za-z0-9 ]", "")],
            "CD_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "NR_NL": [("[^A-Za-z0-9 ]", "")],
            "NR_ANO_NL": [("[^A-Za-z0-9 ]", "")],
            "NR_NE": [("[^A-Za-z0-9 ]", "")],
            "NR_ANO_NE": [("[^A-Za-z0-9 ]", "")],
            "VL_FINAL": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_servidores_df_desp_ord_nt_emp(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CD_UG": [("[^A-Za-z0-9 ]", "")],
            "CD_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "NR_NE": [("[^A-Za-z0-9 ]", "")],
            "NR_ANO_NE": [("[^A-Za-z0-9 ]", "")],
            "CD_UG_LIQ": [("[^A-Za-z0-9 ]", "")],
            "CD_GESTAO_LIQ": [("[^A-Za-z0-9 ]", "")],
            "NR_OB": [("[^A-Za-z0-9 ]", "")],
            "NR_ANO_OB": [("[^A-Za-z0-9 ]", "")],
            "VL_EVENTO": [("[,]", ".")],
            "VL_CANCELADO": [("[,]", ".")],
            "VL_FINAL": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_servidores_df_desp_emp_sub(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "NOTA_EMPENHO": [("[^A-Za-z0-9 ]", "")],
            "ANO_NOTA_EMPENHO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_UG": [("[^A-Za-z0-9 ]", "")],
            "UG": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "GESTAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_SUBELEMENTO": [("[^A-Za-z0-9 ]", "")],
            "NOME_SUBELEMENTO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_NATUREZA": [("[^A-Za-z0-9 ]", "")],
            "VALOR_EMPENHADO": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_servidores_df_desp_emp_desc(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "NUMERO_NOTA_EMPENHO": [("[^A-Za-z0-9 ]", "")],
            "EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "EMISSAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_UG": [("[^A-Za-z0-9 ]", "")],
            "UG": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "GESTAO": [("[^A-Za-z0-9 ]", "")],
            "ITEM": [("[^A-Za-z0-9 ]", "")],
            "MEDIDA": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO": [("[^A-Za-z0-9 ]", "")],
            "QUANTIDADE": [("[^A-Za-z0-9 ]", "")],
            "VALOR_UNITARIO": [("[,]", ".")],
            "VALOR_TOTAL": [("[,]", ".")],
        }

        date_columns = ["EMISSAO"]

        start1 = 1
        len1 = 4

        start2 = 5
        len2 = 2

        start3 = 7
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        dataframe = self.formatting.format_date_columns(
            dataframe=dataframe,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2,
            start3=start3,
            len3=len3,
        )

        return dataframe

    def columns_servidores_df_desp_principal(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_UG": [("[^A-Za-z0-9 ]", "")],
            "UG": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "GESTAO": [("[^A-Za-z0-9 ]", "")],
            "CNPJ_CPF_DO_CREDOR": [("[^A-Za-z0-9 ]", "")],
            "CREDOR": [("[^A-Za-z0-9 ]", "")],
            "PROCESSO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_FONTE_DE_RECURSOS": [("[^A-Za-z0-9 ]", "")],
            "FONTE_DE_RECURSOS": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_ESFERA": [("[^A-Za-z0-9 ]", "")],
            "ESFERA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_FUNCAO": [("[^A-Za-z0-9 ]", "")],
            "FUNCAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_SUBFUNCAO": [("[^A-Za-z0-9 ]", "")],
            "SUBFUNCAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_PROGRAMA": [("[^A-Za-z0-9 ]", "")],
            "PROGRAMA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_ACAO": [("[^A-Za-z0-9 ]", "")],
            "ACAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_SUBTITULO": [("[^A-Za-z0-9 ]", "")],
            "SUBTITULO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_CATEGORIA_ECONOMICA": [("[^A-Za-z0-9 ]", "")],
            "CATEGORIA_ECONOMICA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MODALIDADE_DE_APLICACAO": [("[^A-Za-z0-9 ]", "")],
            "MODALIDADE_DE_APLICACAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_GRUPO_DE_NATUREZA_DESPESA": [("[^A-Za-z0-9 ]", "")],
            "GRUPO_DE_NATUREZA_DESPESA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_ELEMENTO_DESPESA": [("[^A-Za-z0-9 ]", "")],
            "ELEMENTO_DESPESA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_PROGRAMA_DE_TRABALHO": [("[^A-Za-z0-9 ]", "")],
            "PROGRAMA_DE_TRABALHO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_TIPO_DESPESA": [("[^A-Za-z0-9 ]", "")],
            "TIPO_DESPESA": [("[^A-Za-z0-9 ]", "")],
            "EMPENHADO": [("[,]", ".")],
            "LIQUIDADO": [("[,]", ".")],
            "PAGO_EX": [("[,]", ".")],
            "PAGO_RPP": [("[,]", ".")],
            "PAGO_RPNP": [("[,]", ".")],
            "PAGO_RET": [("[,]", ".")],
            "TOTAL_PAGO": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_servidores_df_desp_nt_lanc_evento(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CD_UG": [("[^A-Za-z0-9 ]", "")],
            "CD_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "NR_NL": [("[^A-Za-z0-9 ]", "")],
            "NR_ANO_NL": [("[^A-Za-z0-9 ]", "")],
            "NR_NE": [("[^A-Za-z0-9 ]", "")],
            "NR_ANO_NE": [("[^A-Za-z0-9 ]", "")],
            "NR_LINHA": [("[^A-Za-z0-9 ]", "")],
            "CD_EVENTO": [("[^A-Za-z0-9 ]", "")],
            "CD_INSCRICAO": [("[^A-Za-z0-9 ]", "")],
            "CD_CLASSIFICACAO": [("[^A-Za-z0-9 ]", "")],
            "VL_LANCAMENTO": [("[,]", ".")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe

    def columns_servidores_df_desp_lanc(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "NOTA_LANCAMENTO": [("[^A-Za-z0-9 ]", "")],
            "EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "EMISSAO": [("[^A-Za-z0-9 ]", "")],
            "PRIORIDADE": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_UG": [("[^A-Za-z0-9 ]", "")],
            "UG": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "GESTAO": [("[^A-Za-z0-9 ]", "")],
            "CNPJ_CPF_CREDOR": [("[^A-Za-z0-9 ]", "")],
            "CREDOR": [("[^A-Za-z0-9 ]", "")],
            "PROCESSO": [("[^A-Za-z0-9 ]", "")],
            "CONTRATO": [("[^A-Za-z0-9 ]", "")],
            "TRANSFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_ESPECIE": [("[^A-Za-z0-9 ]", "")],
            "ESPECIE": [("[^A-Za-z0-9 ]", "")],
            "FATURA": [("[^A-Za-z0-9 ]", "")],
            "OBSERVACAO": [("[^A-Za-z0-9 ]", "")],
            "VALOR": [("[,]", ".")],
        }

        date_columns = ["EMISSAO"]

        start1 = 5
        len1 = 4

        start2 = 3
        len2 = 2

        start3 = 1
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        dataframe = self.formatting.format_date_columns(
            dataframe=dataframe,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2,
            start3=start3,
            len3=len3,
        )

        return dataframe

    def columns_servidores_df_desp_pgto(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "ORDEM_BANCARIA": [("[^A-Za-z0-9 ]", "")],
            "EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "EMISSAO": [("[^A-Za-z0-9 ]", "")],
            "HORA": [("[^A-Za-z0-9 ]", "")],
            "PRIORIDADE": [("[^A-Za-z0-9 ]", "")],
            "NL_REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_UG": [("[^A-Za-z0-9 ]", "")],
            "UG": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_UG_LIQUIDANTE": [("[^A-Za-z0-9 ]", "")],
            "UG_LIQUIDANTE": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "GESTAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_GESTAO_NL": [("[^A-Za-z0-9 ]", "")],
            "GESTAO_NL": [("[^A-Za-z0-9 ]", "")],
            "CNPJ_CPF_CREDOR": [("[^A-Za-z0-9 ]", "")],
            "CREDOR": [("[^A-Za-z0-9 ]", "")],
            "PROCESSO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_BANCO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_AGENCIA": [("[^A-Za-z0-9 ]", "")],
            "DOMICILIO_BANCARIO": [("[^A-Za-z0-9 ]", "")],
            "FATURA": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO": [("[^A-Za-z0-9 ]", "")],
            "VALOR_FINAL": [("[,]", ".")],
        }

        date_columns = ["EMISSAO"]

        start1 = 5
        len1 = 4

        start2 = 3
        len2 = 2

        start3 = 1
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        dataframe = self.formatting.format_date_columns(
            dataframe=dataframe,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2,
            start3=start3,
            len3=len3,
        )

        return dataframe

    def columns_servidores_df_desp_emp(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "NOTA_EMPENHO": [("[^A-Za-z0-9 ]", "")],
            "EXERCICIO": [("[^A-Za-z0-9 ]", "")],
            "EMISSAO": [("[^A-Za-z0-9 ]", "")],
            "LANCAMENTO": [("/", "-")],
            "NUMERO_DO_PROCESSO": [("[^A-Za-z0-9 ]", "")],
            "CNPJ_CPF_CREDOR": [("[^A-Za-z0-9 ]", "")],
            "CREDOR": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_UG": [("[^A-Za-z0-9 ]", "")],
            "UG": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_UO": [("[^A-Za-z0-9 ]", "")],
            "UNIDADE_UO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_GESTAO": [("[^A-Za-z0-9 ]", "")],
            "GESTAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_FONTE_RECURSOS": [("[^A-Za-z0-9 ]", "")],
            "FONTE_RECURSOS": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_ESFERA": [("[^A-Za-z0-9 ]", "")],
            "ESFERA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_FUNCAO": [("[^A-Za-z0-9 ]", "")],
            "FUNCAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_SUBFUNCAO": [("[^A-Za-z0-9 ]", "")],
            "SUBFUNCAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_PROGRAMA": [("[^A-Za-z0-9 ]", "")],
            "PROGRAMA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_ACAO": [("[^A-Za-z0-9 ]", "")],
            "ACAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_SUBTITULO": [("[^A-Za-z0-9 ]", "")],
            "SUBTITULO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_CATEGORIA_ECONOMICA": [("[^A-Za-z0-9 ]", "")],
            "CATEGORIA_ECONOMICA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MODALIDADE_APLICACAO": [("[^A-Za-z0-9 ]", "")],
            "MODALIDADE_APLICACAO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_GRUPO_NATUREZA_DA_DESPESA": [("[^A-Za-z0-9 ]", "")],
            "GRUPO_NATUREZA_DESPESA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_ELEMENTO_DESPESA": [("[^A-Za-z0-9 ]", "")],
            "ELEMENTO_DESPESA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_PROGRAMA_DE_TRABALHO": [("[^A-Za-z0-9 ]", "")],
            "PROGRAMA_DE_TRABALHO": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_NATUREZA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_LICITACAO": [("[^A-Za-z0-9 ]", "")],
            "LICITACAO": [("[^A-Za-z0-9 ]", "")],
            "REFERENCIA": [("[^A-Za-z0-9 ]", "")],
            "CODIGO_MODALIDADE_EMPENHO": [("[^A-Za-z0-9 ]", "")],
            "MODALIDADE_EMPENHO": [("[^A-Za-z0-9 ]", "")],
            "CONTRATO": [("[^A-Za-z0-9 ]", "")],
            "PRAZO_ENTREGA": [("[^A-Za-z0-9 ]", "")],
            "LOCAL_ENTREGA": [("[^A-Za-z0-9 ]", "")],
            "VALOR_INICIAL": [("[,]", ".")],
            "VALOR_FINAL": [("[,]", ".")],
        }

        date_columns = ["EMISSAO"]

        start1 = 5
        len1 = 4

        start2 = 3
        len2 = 2

        start3 = 1
        len3 = 2

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        dataframe = self.formatting.format_date_columns(
            dataframe=dataframe,
            columns=date_columns,
            start1=start1,
            len1=len1,
            start2=start2,
            len2=len2,
            start3=start3,
            len3=len3,
        )

        return dataframe

    def columns_servidores_df_patrimonio(self, df_for_clean, file_name):
        """
        This method is responsible for cleaning the columns of the dataframe
        :param dataframe: The dataframe to be cleaned
        :param file_name: The name of the file
        :return: The cleaned dataframe
        """
        regexp_columns = {
            "CODIGO_DE_TOMBAMENTO": [("[^A-Za-z0-9 ]", "")],
            "ITEM": [("[^A-Za-z0-9 ]", "")],
            "DESCRICAO": [("[^A-Za-z0-9 ]", "")],
            "UNIDADE_GESTORA": [("[^A-Za-z0-9 ]", "")],
            "LOCALIZACAO": [("[^A-Za-z0-9 ]", "")],
            "ESTADO": [("[^A-Za-z0-9 ]", "")],
            "QUANTIDADE": [("[^A-Za-z0-9 ]", "")],
            "VALOR_ATUAL": [("[,]", ".")],
            "DATA_INCORPORACAO": [("/", "-")],
        }

        # Applying transformations to specified columns
        dataframe = self.cleaning.process_dataframe(
            df_for_clean, file_name, regexp_columns, self.cleaning.processing_date
        )

        return dataframe
