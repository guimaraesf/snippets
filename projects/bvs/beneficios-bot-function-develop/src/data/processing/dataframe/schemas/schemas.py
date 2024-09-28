#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: schemas.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code is responsible for creating the schemas of all tables
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
from pyspark.sql.types import StructType, StructField, StringType

# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))
# pylint: disable=import-error
from columns import *
from variables import *


class Schemas:
    """
    This class is responsible for creating the schemas of all tables
    """

    def __init__(self, variables=Variables):
        self.variables = variables
        self.schemas = {}
        self.schemas_tasks = {
            self.variables.FILENAME_AUXBRASIL: select_columns_AuxilioBrasil,
            self.variables.FILENAME_AUXEMERG: select_columns_AuxilioEmergencial,
            self.variables.FILENAME_NOVO_BOLSA_FAMILIA: select_columns_Novo_BolsaFamilia,
            self.variables.FILENAME_BOLSA_FAMILIA_SQ: select_columns_BolsaFamilia_Saques,
            self.variables.FILENAME_BOLSA_FAMILIA_PGTO: select_columns_BolsaFamilia_Pagamentos,
            self.variables.FILENAME_PETI: select_columns_PETI,
            self.variables.FILENAME_BPC: select_columns_BPC,
            self.variables.FILENAME_GARANTIA_SAFRA: select_columns_GarantiaSafra,
            self.variables.FILENAME_SEGURO_DEFESO: select_columns_SeguroDefeso,
            self.variables.FILENAME_TRAB_FORMAL: select_columns_TrabFormal,
            self.variables.FILENAME_EMPR_DOMESTICO: select_columns_EmprDomestico,
            self.variables.FILENAME_TRAB_RESGATADO: select_columns_TrabResgatado,
            self.variables.FILENAME_BOLSA_QUALIFIC: select_columns_BolsaQualific,
            self.variables.FILENAME_PESC_ARTESANAL: select_columns_PescArtesanal,
            self.variables.FILENAME_APOSENTADOS_BACEN: select_columns_Aposentados,
            self.variables.FILENAME_APOSENTADOS_SIAPE: select_columns_Aposentados,
            self.variables.FILENAME_MILITARES: select_columns_Militares,
            self.variables.FILENAME_PENSIONISTAS_SIAPE: select_columns_Pensionistas,
            self.variables.FILENAME_PENSIONISTAS_BACEN: select_columns_Pensionistas,
            self.variables.FILENAME_PENSIONISTAS_DEFESA: select_columns_Pensionistas,
            self.variables.FILENAME_RESERVA_MILITARES: select_columns_Reserva_Ref_Militares,
            self.variables.FILENAME_SERVIDORES_BACEN: select_columns_Servidores,
            self.variables.FILENAME_SERVIDORES_SIAPE: select_columns_Servidores,
            self.variables.FILENAME_HONORARIOS_ADVOC: select_columns_Honorarios_Advoc,
            self.variables.FILENAME_HONORARIOS_JETONS: select_columns_Honorarios_Jetons,
            self.variables.FILENAME_SERVIDORES_SP: select_columns_Servidores_SP,
            self.variables.FILENAME_SERVIDORES_MG: select_columns_Servidores_MG,
            self.variables.FILENAME_SERVIDORES_ES: select_columns_Servidores_ES,
            self.variables.FILENAME_SERVIDORES_SC: select_columns_Servidores_SC,
            self.variables.FILENAME_SERVIDORES_PR: select_columns_Servidores_PR,
            self.variables.FILENAME_CEIS: select_columns_CEIS,
            self.variables.FILENAME_CEPIM: select_columns_CEPIM,
            self.variables.FILENAME_CNEP: select_columns_CNEP,
            self.variables.FILENAME_ACORDOS_LENIENCIA: select_columns_AcordosLeniencia,
            self.variables.FILENAME_EFEITOS_LENIENCIA: select_columns_EfeitosLeniencia,
            self.variables.FILENAME_IBAMA: select_columns_Ibama,
            self.variables.FILENAME_FORNECEDORES_COMPRAS: select_columns_fornecedores_compras,
            self.variables.FILENAME_FORNECEDORES_ITEM_COMPRA: select_columns_fornecedores_item_compra,
            self.variables.FILENAME_FORNECEDORES_TERMO_ADITIVO: select_columns_fornecedores_termo_aditivo,
            self.variables.FILENAME_CNPJ_CNAES: select_columns_Cnaes,
            self.variables.FILENAME_CNPJ_EMPRESAS: select_columns_Empresas,
            self.variables.FILENAME_CNPJ_ESTABELECIMENTOS: select_columns_Estabelecimentos,
            self.variables.FILENAME_CNPJ_SOCIOS: select_columns_Socios,
            self.variables.FILENAME_CNPJ_MOTIVOS: select_columns_Motivos,
            self.variables.FILENAME_CNPJ_MUNICIPIOS: select_columns_Municipios,
            self.variables.FILENAME_CNPJ_NATUREZA_JURIDICA: select_columns_NaturezaJuridica,
            self.variables.FILENAME_CNPJ_PAISES: select_columns_Paises,
            self.variables.FILENAME_CNPJ_QUALIFICACOES_SOCIOS: select_columns_QualificoesSocios,
            self.variables.FILENAME_CNPJ_SIMPLES: select_columns_Simples,
            self.variables.FILENAME_CNPJ_IMUNES: select_columns_ImunesIsentas,
            self.variables.FILENAME_CNPJ_LUCRO_ARBITRADO: select_columns_LucroArbitrado,
            self.variables.FILENAME_CNPJ_LUCRO_REAL: select_columns_LucroReal,
            self.variables.FILENAME_CNPJ_LUCRO_PRESUMIDO: select_columns_LucroPresumido,
            self.variables.FILENAME_DEBITOS_PREVIDENCIARIO: select_columns_Debitos_Prev,
            self.variables.FILENAME_DEBITOS_NAO_PREVIDENCIARIO: select_columns_Debitos_Nao_Prev,
            self.variables.FILENAME_DEBITOS_FGTS: select_columns_Debitos_FGTS,
            self.variables.FILENAME_DESP_EMPENHO: select_columns_Servidores_DF_Desp_Emp,
            self.variables.FILENAME_DESP_EMPENHO_DESC: select_columns_Servidores_DF_Desp_Emp_Descricao,
            self.variables.FILENAME_DESP_EMPENHO_SUB: select_columns_Servidores_DF_Desp_Emp_Subelemento,
            self.variables.FILENAME_DESP_LANCAMENTO: select_columns_Servidores_DF_Desp_Lanc,
            self.variables.FILENAME_DESP_NT_LANC_NT_EMPENHO: select_columns_Servidores_DF_Desp_Nota_Lanc_Emp,
            self.variables.FILENAME_DESP_NT_LANC_EVENTO: select_columns_Servidores_DF_Desp_Nota_Lanc_Evento,
            self.variables.FILENAME_DESP_ORD_BANC_CANCELADA: select_columns_Servidores_DF_Desp_Ord_Banc_Cancelada,
            self.variables.FILENAME_DESP_ORD_BANC_NT_EMPENHO: select_columns_Servidores_DF_Desp_Ord_Banc_Nota_Emp,
            self.variables.FILENAME_DESP_PAGAMENTO: select_columns_Servidores_DF_Desp_Pgto,
            self.variables.FILENAME_DESP_PRINCIPAL: select_columns_Servidores_DF_Desp_Principal,
            self.variables.FILENAME_LICITACAO: select_columns_Servidores_DF_Licitacao,
            self.variables.FILENAME_SERVIDORES_ORGAO: select_columns_Servidores_DF_Orgao,
            self.variables.FILENAME_PATRIMONIO: select_columns_Servidores_DF_Patrimonio,
            self.variables.FILENAME_RECEITA: select_columns_Servidores_DF_Receita,
            self.variables.FILENAME_REMUNERACAO: select_columns_Servidores_DF_Remuneracao,
            self.variables.FILENAME_REMUNERACAO_DETALHAMENTO: select_columns_Servidores_DF_Remuneracao_Detalhamento,
        }

    @staticmethod
    def __create_schema(columns):
        schema = StructType(
            [StructField(column, StringType(), True) for column in columns]
        )
        return schema

    def __add_schema(self, file, schema):
        self.schemas[file] = schema

    def get_all_schemas(self):
        """
        Returns all schemas
        """
        # Loop through each file and its associated columns
        for file, columns in self.schemas_tasks.items():
            schema = self.__create_schema(columns)
            self.__add_schema(file, schema)
        return self.schemas
