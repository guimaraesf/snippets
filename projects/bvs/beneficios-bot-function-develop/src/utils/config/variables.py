#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: variables.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code centralizes all variables to use in other modules
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
import inspect

# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))


class Variables(enumerate):
    """
    Class to set all support variables.
    """

    FILENAME_AUXBRASIL: str = "AuxilioBrasil.csv"
    FILENAME_AUXEMERG: str = "AuxilioEmergencial.csv"
    FILENAME_NOVO_BOLSA_FAMILIA: str = "NovoBolsaFamilia.csv"
    FILENAME_BOLSA_FAMILIA_SQ: str = "BolsaFamilia_Saques.csv"
    FILENAME_BOLSA_FAMILIA_PGTO: str = "BolsaFamilia_Pagamentos.csv"
    FILENAME_PETI: str = "PETI.csv"
    FILENAME_BPC: str = "BPC.csv"
    FILENAME_GARANTIA_SAFRA: str = "GarantiaSafra.csv"
    FILENAME_SEGURO_DEFESO: str = "SeguroDefeso.csv"
    FILENAME_TRAB_FORMAL: str = "TrabalhadorFormal.csv"
    FILENAME_EMPR_DOMESTICO: str = "EmpregadoDomestico.csv"
    FILENAME_TRAB_RESGATADO: str = "TrabalhadorResgatado.csv"
    FILENAME_BOLSA_QUALIFIC: str = "BolsaQualificacao.csv"
    FILENAME_PESC_ARTESANAL: str = "PescadorArtesanal.csv"
    FILENAME_APOSENTADOS_BACEN: str = "Aposentados_BACEN.csv"
    FILENAME_APOSENTADOS_SIAPE: str = "Aposentados_SIAPE.csv"
    FILENAME_MILITARES: str = "Militares.csv"
    FILENAME_PENSIONISTAS_SIAPE: str = "Pensionistas_SIAPE.csv"
    FILENAME_PENSIONISTAS_BACEN: str = "Pensionistas_BACEN.csv"
    FILENAME_PENSIONISTAS_DEFESA: str = "Pensionistas_DEFESA.csv"
    FILENAME_RESERVA_MILITARES: str = "Reserva_Reforma_Militares.csv"
    FILENAME_SERVIDORES_BACEN: str = "Servidores_BACEN.csv"
    FILENAME_SERVIDORES_SIAPE: str = "Servidores_SIAPE.csv"
    FILENAME_HONORARIOS_ADVOC: str = "Honorarios_Advocaticios.csv"
    FILENAME_HONORARIOS_JETONS: str = "Honorarios_Jetons.csv"
    FILENAME_SERVIDORES_SP: str = "Servidores_SP.csv"
    FILENAME_SERVIDORES_MG: str = "Servidores_MG.csv"
    FILENAME_SERVIDORES_ES: str = "Servidores_ES.csv"
    FILENAME_SERVIDORES_SC: str = "Servidores_SC.csv"
    FILENAME_SERVIDORES_PR: str = "Servidores_PR.csv"
    FILENAME_FORNECEDORES_COMPRAS: str = "Compras.csv"
    FILENAME_FORNECEDORES_ITEM_COMPRA: str = "ItemCompra.csv"
    FILENAME_FORNECEDORES_TERMO_ADITIVO: str = "TermoAditivo.csv"
    FILENAME_CNPJ_CNAES: str = "Cnaes.csv"
    FILENAME_CNPJ_EMPRESAS: str = "Empresas"
    FILENAME_CNPJ_ESTABELECIMENTOS: str = "Estabelecimento"
    FILENAME_CNPJ_SOCIOS: str = "Socios"
    FILENAME_CNPJ_MOTIVOS: str = "Motivos.csv"
    FILENAME_CNPJ_MUNICIPIOS: str = "Municipios.csv"
    FILENAME_CNPJ_NATUREZA_JURIDICA: str = "Naturezas.csv"
    FILENAME_CNPJ_PAISES: str = "Paises.csv"
    FILENAME_CNPJ_QUALIFICACOES_SOCIOS: str = "Qualificacoes.csv"
    FILENAME_CNPJ_SIMPLES: str = "Simples.csv"
    FILENAME_CNPJ_IMUNES: str = "ImunesIsentas.csv"
    FILENAME_CNPJ_LUCRO_ARBITRADO: str = "LucroArbitrado.csv"
    FILENAME_CNPJ_LUCRO_REAL: str = "LucroReal.csv"
    FILENAME_CNPJ_LUCRO_PRESUMIDO: str = "LucroPresumido.csv"
    FILENAME_DEBITOS_PREVIDENCIARIO: str = "Previdenciario"
    FILENAME_DEBITOS_NAO_PREVIDENCIARIO: str = "Nao_Previdenciario"
    FILENAME_DEBITOS_FGTS: str = "FGTS"
    FILENAME_CEIS: str = "CEIS.csv"
    FILENAME_CEPIM: str = "CEPIM.csv"
    FILENAME_CNEP: str = "CNEP.csv"
    FILENAME_ACORDOS_LENIENCIA: str = "Acordos.csv"
    FILENAME_EFEITOS_LENIENCIA: str = "Efeitos.csv"
    FILENAME_IBAMA: str = "Ibama.csv"
    FILENAME_DESP_EMPENHO: str = "Despesa_Empenho.csv"
    FILENAME_DESP_EMPENHO_DESC: str = "Despesa_Empenho_Descricao.csv"
    FILENAME_DESP_EMPENHO_SUB: str = "Despesa_Empenho_Subelemento.csv"
    FILENAME_DESP_LANCAMENTO: str = "Despesa_Lancamento.csv"
    FILENAME_DESP_NT_LANC_NT_EMPENHO: str = "Despesa_Nota_Lancamento_x_Nota_Empenho.csv"
    FILENAME_DESP_NT_LANC_EVENTO: str = "Despesa_Nota_Lancamento_Evento.csv"
    FILENAME_DESP_ORD_BANC_CANCELADA: str = "Despesa_Ordem_Bancaria_Cancelada.csv"
    FILENAME_DESP_ORD_BANC_NT_EMPENHO: str = "Despesa_Ordem_Bancaria_x_Nota_Empenho.csv"
    FILENAME_DESP_PAGAMENTO: str = "Despesa_Pagamento.csv"
    FILENAME_DESP_PRINCIPAL: str = "Despesa_Principal.csv"
    FILENAME_LICITACAO: str = "Licitacao.csv"
    FILENAME_SERVIDORES_ORGAO: str = "Servidores_Orgao.csv"
    FILENAME_PATRIMONIO: str = "Patrimonio_Bens_Moveis.csv"
    FILENAME_RECEITA: str = "Receita.csv"
    FILENAME_REMUNERACAO: str = "Remuneracao.csv"
    FILENAME_REMUNERACAO_DETALHAMENTO: str = "Remuneracao_Detalhamento.csv"

    # This list contains the same environment variables that were configured in the Cloud Function.
    FUNCTION_ENV_VARS: list = [
        "WORKFLOW_JOB",
        "BUCKET_INITIALIZATION_ACTIONS",
        "BUCKET_JOBS_NAME",
        "BUCKET_NAME",
        "CLUSTER_NAME",
        "DATASET",
        "DISK_SIZE_GB",
        "ENVIRONMENT",
        "LABEL_SQUAD",
        "LABEL_VALUE_STREAM",
        "LABEL_COST_CENTER",
        "LABEL_APPLICATION",
        "MASTER_NUM_INSTANCES",
        "MACHINE_TYPE_URI",
        "PROJECT_ID",
        "PROJECT_NAME",
        "REGION",
        "SERVICE_ACCOUNT",
        "SPARK_APP_NAME",
        "SPARK_MASTER",
        "SUBNETWORK",
        "TMP_DIR",
        "TOPIC_NAME",
        "WORKER_NUM_INSTANCES",
        "TABLE_NAME_SANCOES_CEIS",
        "TABLE_NAME_SANCOES_CEPIM",
        "TABLE_NAME_SANCOES_CNEP",
        "TABLE_NAME_SANCOES_ACORDOS",
        "TABLE_NAME_SANCOES_EFEITOS",
        "TABLE_NAME_IBAMA",
        "TABLE_NAME_AUX_BRASIL",
        "TABLE_NAME_AUX_EMERG",
        "TABLE_NAME_BF_PAGAMENTOS",
        "TABLE_NAME_BF_SAQUES",
        "TABLE_NAME_BPC",
        "TABLE_NAME_GARANTIAS_SAFRA",
        "TABLE_NAME_PETI",
        "TABLE_NAME_SEGURO_DEFESO",
        "TABLE_NAME_TRAB_FORMAL",
        "TABLE_NAME_EMPR_DOMESTICO",
        "TABLE_NAME_TRAB_RESGATADO",
        "TABLE_NAME_BOLSA_QUALIFIC",
        "TABLE_NAME_PESC_ARTESANAL",
        "TABLE_NAME_APOSENTADOS",
        "TABLE_NAME_HONOR_ADVOC",
        "TABLE_NAME_HONOR_JETONS",
        "TABLE_NAME_MILITARES",
        "TABLE_NAME_PENSIONISTAS",
        "TABLE_NAME_RESERVA_REF_MILITARES",
        "TABLE_NAME_SERVIDORES",
        "TABLE_NAME_SERVIDORES_SP",
        "TABLE_NAME_SERVIDORES_MG",
        "TABLE_NAME_SERVIDORES_ES",
        "TABLE_NAME_FORNECEDORES_COMPRAS",
        "TABLE_NAME_FORNECEDORES_ITEM_COMPRA",
        "TABLE_NAME_FORNECEDORES_TERMO_ADITIVO",
        "TABLE_NAME_CNPJ_CNAES",
        "TABLE_NAME_CNPJ_EMPRESAS",
        "TABLE_NAME_CNPJ_ESTABELECIMENTOS",
        "TABLE_NAME_CNPJ_IMUNE_ISENTAS",
        "TABLE_NAME_CNPJ_LUCRO_ARBITRADO",
        "TABLE_NAME_CNPJ_LUCRO_PRESUMIDO",
        "TABLE_NAME_CNPJ_LUCRO_REAL",
        "TABLE_NAME_CNPJ_MOTIVOS",
        "TABLE_NAME_CNPJ_MUNICIPIOS",
        "TABLE_NAME_CNPJ_NATUREZA_JURIDICA",
        "TABLE_NAME_CNPJ_PAISES",
        "TABLE_NAME_CNPJ_QUALIFICACAO_SOCIO",
        "TABLE_NAME_CNPJ_SIMPLES",
        "TABLE_NAME_CNPJ_SOCIOS",
        "TABLE_NAME_SERVIDORES_SC",
        "TABLE_NAME_DEBITOS_PREV",
        "TABLE_NAME_DEBITOS_NAO_PREV",
        "TABLE_NAME_DEBITOS_FGTS",
        "TABLE_NAME_SERVIDORES_PR",
        "TABLE_NAME_NOVO_BF",
        "TABLE_NAME_SERVIDORES_DF_DESP_EMPENHO",
        "TABLE_NAME_SERVIDORES_DF_DESP_EMPENHO_DESC",
        "TABLE_NAME_SERVIDORES_DF_DESP_EMPENHO_SUB",
        "TABLE_NAME_SERVIDORES_DF_DESP_LANCAMENTO",
        "TABLE_NAME_SERVIDORES_DF_DESP_NT_LANC",
        "TABLE_NAME_SERVIDORES_DF_DESP_NT_LANC_EVENTO",
        "TABLE_NAME_SERVIDORES_DF_DESP_ORD_CANCELADA",
        "TABLE_NAME_SERVIDORES_DF_DESP_ORD_EMPENHO",
        "TABLE_NAME_SERVIDORES_DF_DESP_PGTO",
        "TABLE_NAME_SERVIDORES_DF_DESP_PRINCIPAL",
        "TABLE_NAME_SERVIDORES_DF_LICITACAO",
        "TABLE_NAME_SERVIDORES_DF_ORGAO",
        "TABLE_NAME_SERVIDORES_DF_PATRIMONIO",
        "TABLE_NAME_SERVIDORES_DF_RECEITA",
        "TABLE_NAME_SERVIDORES_DF_REMUNERACAO",
        "TABLE_NAME_SERVIDORES_DF_REMUNERACAO_DET",
    ]

    # This list contains the files that are executed within the bucket when the Workflow Template is started.
    PYTHON_FILES: list = [
        "src/data/ingestion/ingestion.py",
        "src/data/ingestion/tasks/task.py",
        "src/data/acquisition/scraping/file_downloader.py",
        "src/data/acquisition/scraping/file_processing.py",
        "src/data/acquisition/scraping/file_unpacking.py",
        "src/data/acquisition/scraping/file_validation.py",
        "src/data/processing/dataframe/columns/columns.py",
        "src/data/processing/dataframe/functions/functions.py",
        "src/data/processing/dataframe/cleaning/cleaning.py",
        "src/data/processing/dataframe/schemas/schemas.py",
        "src/utils/config/app_log.py",
        "src/utils/config/spark.py",
        "src/utils/config/variables.py",
        "src/utils/gcp/bigquery/bigquery.py",
        "src/utils/gcp/dataproc/template.py",
        "src/utils/gcp/dataproc/workflow_pyspark_jobs.py",
        "src/utils/gcp/dataproc/workflow_submit_jobs.py",
        "src/utils/gcp/dataproc/workflow_vars.py",
        "src/utils/gcp/storage/storage.py",
        "src/utils/gcp/storage/terraform_prep.py",
        "src/utils/date_utils.py",
        "src/utils/utils.py",
    ]
    # These are the environment variables inserted in the context of the data capture job.
    ACQUISITION_ARGS: list = [
        "workflow_job",
        "message",
        "bucket_name",
        "project_id",
        "tmp_dir",
    ]
    # These are the environment variables inserted in the context of the data ingestion job.
    INGESTION_ARGS: list = [
        "workflow_job",
        "message",
        "step_name",
        "bucket_name",
        "dataset",
        "project_id",
        "project_name",
        "spark_app_name",
        "spark_master",
        "tmp_dir",
        "table_sancoes_ceis",
        "table_sancoes_cepim",
        "table_sancoes_cnep",
        "table_sancoes_acordos_leniencia",
        "table_sancoes_efeitos_leniencia",
        "table_ibama",
        "table_auxilio_brasil",
        "table_auxilio_emergencial",
        "table_bolsafamilia_pa",
        "table_bolsafamilia_sa",
        "table_bpc",
        "table_garantias_safra",
        "table_peti",
        "table_seguro_defeso",
        "table_trab_formal",
        "table_empr_domestico",
        "table_trab_resgatado",
        "table_bolsa_qualific",
        "table_pesc_artesanal",
        "table_aposentados",
        "table_honorarios_advoc",
        "table_honorarios_jetons",
        "table_militares",
        "table_pensionistas",
        "table_reserva_ref_militares",
        "table_servidores_federais",
        "table_servidores_sp",
        "table_servidores_mg",
        "table_servidores_es",
        "table_fornecedores_compras",
        "table_fornecedores_item_compra",
        "table_fornecedores_termo_aditivo",
        "table_cnpj_cnaes",
        "table_cnpj_empresas",
        "table_cnpj_estabelecimentos",
        "table_cnpj_imunes",
        "table_cnpj_lucro_arbitrado",
        "table_cnpj_lucro_presumido",
        "table_cnpj_lucro_real",
        "table_cnpj_motivos",
        "table_cnpj_municipios",
        "table_cnpj_natureza_juridica",
        "table_cnpj_paises",
        "table_cnpj_qualific_socios",
        "table_cnpj_simples",
        "table_cnpj_socios",
        "table_servidores_sc",
        "table_debitos_prev",
        "table_debitos_nao_prev",
        "table_debitos_fgts",
        "table_servidores_pr",
        "table_novo_bolsa_familia",
        "table_servidores_df_desp_emp",
        "table_servidores_df_desp_emp_desc",
        "table_servidores_df_desp_emp_sub",
        "table_servidores_df_desp_lanc",
        "table_servidores_df_desp_nt_lanc_emp",
        "table_servidores_df_desp_nt_lanc_evento",
        "table_servidores_df_desp_ord_cancelada",
        "table_servidores_df_desp_ord_nt_emp",
        "table_servidores_df_desp_pagamento",
        "table_servidores_df_desp_principal",
        "table_servidores_df_licitacao",
        "table_servidores_df_orgao",
        "table_servidores_df_patrimonio",
        "table_servidores_df_receita",
        "table_servidores_df_remuneracao",
        "table_servidores_df_remuneracao_det",
    ]
    # This dictionary contains all jobs to be executed in Dataproc.
    JOBS: dict = {
        "auxilio-brasil": "auxilio_brasil",
        "auxilio-emergencial": "auxilio_emergencial",
        "bpc": "bpc",
        "garantia-safra": "garantia_safra",
        "novo-bolsa-familia": "novo_bolsa_familia",
        "peti": "peti",
        "seguro-defeso": "seguro_defeso",
        "cnpj-cadastros": "cnpj_cadastros",
        "cnpj-regime_tributario": "cnpj_regime_tributario",
        "debitos-trabalhistas": "debitos_trabalhistas",
        "fornecedores_gov": "fornecedores_gov",
        "seguro_desemprego": "seguro_desemprego",
        "servidores-es": "servidores_es",
        "servidores-mg": "servidores_mg",
        "servidores-pr": "servidores_pr",
        "servidores-sc": "servidores_sc",
        "servidores-sp": "servidores_sp",
        "servidores-df-despesa": "servidores_df_despesa",
        "servidores-df-patrimonio": "servidores_df_patrimonio",
        "servidores-df-licitacao": "servidores_df_licitacao",
        "servidores-df-receita": "servidores_df_receita",
        "servidores-df-orgao": "servidores_df_orgao",
        "servidores-df-remuneracao": "servidores_df_remuneracao",
        "servidores-federais": "servidores_federais",
        "sancoes": "sancoes",
        "ibama": "ibama",
    }

    # This list is needed to create the paths in the bucket.
    # This is required in advance to be able to provision the external table via Terraform.
    TRUSTED_BLOB_PATHS: list = [
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/DESPESA/EMPENHO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/DESPESA/EMPENHODESCRICAO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/DESPESA/EMPENHOSUBELEMENTO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/DESPESA/LANCAMENTO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/DESPESA/NOTALANCAMENTOEMPENHO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/DESPESA/NOTALANCAMENTOEVENTO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/DESPESA/ORDEMBANCARIACANCELADA",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/DESPESA/ORDEMBANCARIAEMPENHO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/DESPESA/PAGAMENTO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/DESPESA/PRINCIPAL",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/LICITACAO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/ORGAO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/PATRIMONIO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/RECEITA",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/REMUNERACAO/REMUNERACAO",
        "SERVIDORES/ESTADUAIS/TRUSTED/DF/REMUNERACAO/REMUNERACAODETALHAMENTO",
    ]

    @staticmethod
    def get_all_variables() -> list:
        """
            Retrieves all class-level variables of the Variables class.
        Returns:
            A list of tuples, each representing a class-level variable and its value.
        """

        attributes = inspect.getmembers(
            Variables, lambda attr: not (inspect.isroutine(attr))
        )
        return [
            v for v in attributes if not (v[0].startswith("__") and v[0].endswith("__"))
        ]


def check_variables(variable) -> None:
    """
        Check if all variables defined in the Variables class have been initialized.

    Returns:
        None
    """

    for key, value in variable.get_all_variables():
        if not value:
            print(f"The variable {key} was not created, because the value is: {value}")


check_variables(Variables)
