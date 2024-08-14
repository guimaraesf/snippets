#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: dataproc.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code is ...
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))
from src.utils.config.variables import Variables
from src.utils.utils import EnvironmentUtils

# Getting all environment variables for dataproc job
env_utils = EnvironmentUtils()
function_env_vars: dict = env_utils.getenv(Variables.FUNCTION_ENV_VARS)

class WorkflowVariables:
    """
    Class to store cron job variables.
    """
    def __init__(
        self,
        env_vars=function_env_vars,
        list_of_files=Variables.PYTHON_FILES
    ) -> None:
        self.env_vars = env_vars
        self.list_of_files = list_of_files
        self.master_num_instances = self.env_vars.get("MASTER_NUM_INSTANCES")
        self.machine_type_uri = self.env_vars.get("MACHINE_TYPE_URI")
        self.worker_num_instances = self.env_vars.get("WORKER_NUM_INSTANCES")
        self.disk_size_gb = self.env_vars.get("DISK_SIZE_GB")
        self.label_application = self.env_vars.get("LABEL_APPLICATION")
        self.label_cost_center = self.env_vars.get("LABEL_COST_CENTER")
        self.label_value_stream = self.env_vars.get("LABEL_VALUE_STREAM")
        self.label_squad = self.env_vars.get("LABEL_SQUAD")
        self.label_environment = self.env_vars.get("ENVIRONMENT")
        self.project_id = self.env_vars.get("PROJECT_ID")
        self.project_name = self.env_vars.get("PROJECT_NAME")
        self.cluster_name = self.env_vars.get("CLUSTER_NAME")
        self.bucket_name = self.env_vars.get("BUCKET_NAME")
        self.dataset = self.env_vars.get("DATASET")
        self.bucket_jobs_name = self.env_vars.get("BUCKET_JOBS_NAME")
        self.region = self.env_vars.get("REGION")
        self.topic_name = self.env_vars.get("TOPIC_NAME")
        self.tmp_dir = self.env_vars.get("TMP_DIR")
        self.spark_app_name = self.env_vars.get("SPARK_APP_NAME")
        self.spark_master = self.env_vars.get("SPARK_MASTER")
        self.service_account = self.env_vars.get("SERVICE_ACCOUNT")
        self.subnetwork = self.env_vars.get("SUBNETWORK")
        self.bucket_init_actions_name = self.env_vars.get("BUCKET_INITIALIZATION_ACTIONS")
        self.table_name_aux_brasil = self.env_vars.get("TABLE_NAME_AUX_BRASIL")
        self.table_name_aux_emerg = self.env_vars.get("TABLE_NAME_AUX_EMERG")
        self.table_name_bf_pagamentos = self.env_vars.get("TABLE_NAME_BF_PAGAMENTOS")
        self.table_name_bf_saques = self.env_vars.get("TABLE_NAME_BF_SAQUES")
        self.table_name_bpc = self.env_vars.get("TABLE_NAME_BPC")
        self.table_name_garantias_safra = self.env_vars.get("TABLE_NAME_GARANTIAS_SAFRA")
        self.table_name_peti = self.env_vars.get("TABLE_NAME_PETI")
        self.table_name_seguro_defeso = self.env_vars.get("TABLE_NAME_SEGURO_DEFESO")
        self.table_name_trab_formal = self.env_vars.get("TABLE_NAME_TRAB_FORMAL")
        self.table_name_empr_domestico = self.env_vars.get("TABLE_NAME_EMPR_DOMESTICO")
        self.table_name_trab_resgatado = self.env_vars.get("TABLE_NAME_TRAB_RESGATADO")
        self.table_name_bolsa_qualific = self.env_vars.get("TABLE_NAME_BOLSA_QUALIFIC")
        self.table_name_pesc_artesanal = self.env_vars.get("TABLE_NAME_PESC_ARTESANAL")
        self.table_name_aposentados = self.env_vars.get("TABLE_NAME_APOSENTADOS")
        self.table_name_honor_advoc = self.env_vars.get("TABLE_NAME_HONOR_ADVOC")
        self.table_name_honor_jetons = self.env_vars.get("TABLE_NAME_HONOR_JETONS")
        self.table_name_militares = self.env_vars.get("TABLE_NAME_MILITARES")
        self.table_name_pensionistas = self.env_vars.get("TABLE_NAME_PENSIONISTAS")
        self.table_name_reserva_ref_militares = self.env_vars.get("TABLE_NAME_RESERVA_REF_MILITARES")
        self.table_name_servidores = self.env_vars.get("TABLE_NAME_SERVIDORES")
        self.table_name_servidores_sp = self.env_vars.get("TABLE_NAME_SERVIDORES_SP")
        self.table_name_servidores_mg = self.env_vars.get("TABLE_NAME_SERVIDORES_MG")
        self.table_name_servidores_es = self.env_vars.get("TABLE_NAME_SERVIDORES_ES")
        self.table_name_fornecedores_compras = self.env_vars.get("TABLE_NAME_FORNECEDORES_COMPRAS")
        self.table_name_fornecedores_item_compra = self.env_vars.get("TABLE_NAME_FORNECEDORES_ITEM_COMPRA")
        self.table_name_fornecedores_termo_aditivo = self.env_vars.get("TABLE_NAME_FORNECEDORES_TERMO_ADITIVO")
        self.table_name_cnpj_cnaes = self.env_vars.get("TABLE_NAME_CNPJ_CNAES")
        self.table_name_cnpj_empresas = self.env_vars.get("TABLE_NAME_CNPJ_EMPRESAS")
        self.table_name_cnpj_estabelecimentos = self.env_vars.get("TABLE_NAME_CNPJ_ESTABELECIMENTOS")
        self.table_name_cnpj_imune_isentas = self.env_vars.get("TABLE_NAME_CNPJ_IMUNE_ISENTAS")
        self.table_name_cnpj_lucro_arbitrado = self.env_vars.get("TABLE_NAME_CNPJ_LUCRO_ARBITRADO")
        self.table_name_cnpj_lucro_presumido = self.env_vars.get("TABLE_NAME_CNPJ_LUCRO_PRESUMIDO")
        self.table_name_cnpj_lucro_real = self.env_vars.get("TABLE_NAME_CNPJ_LUCRO_REAL")
        self.table_name_cnpj_motivos = self.env_vars.get("TABLE_NAME_CNPJ_MOTIVOS")
        self.table_name_cnpj_municipios = self.env_vars.get("TABLE_NAME_CNPJ_MUNICIPIOS")
        self.table_name_cnpj_natureza_juridica = self.env_vars.get("TABLE_NAME_CNPJ_NATUREZA_JURIDICA")
        self.table_name_cnpj_paises = self.env_vars.get("TABLE_NAME_CNPJ_PAISES")
        self.table_name_cnpj_qualificacao_socio = self.env_vars.get("TABLE_NAME_CNPJ_QUALIFICACAO_SOCIO")
        self.table_name_cnpj_simples = self.env_vars.get("TABLE_NAME_CNPJ_SIMPLES")
        self.table_name_cnpj_socios = self.env_vars.get("TABLE_NAME_CNPJ_SOCIOS")
        self.table_name_servidores_sc = self.env_vars.get("TABLE_NAME_SERVIDORES_SC")
        self.table_name_debitos_prev = self.env_vars.get("TABLE_NAME_DEBITOS_PREV")
        self.table_name_debitos_nao_prev = self.env_vars.get("TABLE_NAME_DEBITOS_NAO_PREV")
        self.table_name_debitos_fgts = self.env_vars.get("TABLE_NAME_DEBITOS_FGTS")
        self.table_name_servidores_pr = self.env_vars.get("TABLE_NAME_SERVIDORES_PR")
        self.table_name_novo_bf = self.env_vars.get("TABLE_NAME_NOVO_BF")
        self.table_name_ibama = self.env_vars.get("TABLE_NAME_IBAMA")
        self.table_name_sancoes_ceis = self.env_vars.get("TABLE_NAME_SANCOES_CEIS")
        self.table_name_sancoes_cepim = self.env_vars.get("TABLE_NAME_SANCOES_CEPIM")
        self.table_name_sancoes_cnep = self.env_vars.get("TABLE_NAME_SANCOES_CNEP")
        self.table_name_sancoes_acordos = self.env_vars.get("TABLE_NAME_SANCOES_ACORDOS")
        self.table_name_sancoes_efeitos = self.env_vars.get("TABLE_NAME_SANCOES_EFEITOS")
        self.table_name_servidores_df_desp_emp = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_DESP_EMPENHO")
        self.table_name_servidores_df_desp_emp_desc = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_DESP_EMPENHO_DESC")
        self.table_name_servidores_df_desp_emp_sub = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_DESP_EMPENHO_SUB")
        self.table_name_servidores_df_desp_lanc = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_DESP_LANCAMENTO")
        self.table_name_servidores_df_desp_nt_lanc = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_DESP_NT_LANC")
        self.table_name_servidores_df_desp_nt_lanc_evento = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_DESP_NT_LANC_EVENTO")
        self.table_name_servidores_df_desp_ord_canc = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_DESP_ORD_CANCELADA")
        self.table_name_servidores_df_desp_ord_emp = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_DESP_ORD_EMPENHO")
        self.table_name_servidores_df_desp_pgto = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_DESP_PGTO")
        self.table_name_servidores_df_desp_principal = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_DESP_PRINCIPAL")
        self.table_name_servidores_df_licitacao = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_LICITACAO")
        self.table_name_servidores_df_orgao = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_ORGAO")
        self.table_name_servidores_df_patrimonio = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_PATRIMONIO")
        self.table_name_servidores_df_receita = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_RECEITA")
        self.table_name_servidores_df_remuneracao = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_REMUNERACAO")
        self.table_name_servidores_df_remuneracao_det = self.env_vars.get("TABLE_NAME_SERVIDORES_DF_REMUNERACAO_DET")
        self.python_file_uris = [
            f"gs://{self.bucket_jobs_name}/{py_file}" 
            for py_file in self.list_of_files
        ]