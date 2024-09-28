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
from src.utils.gcp.dataproc.workflow_pyspark_jobs import PySparkJob
from src.utils.gcp.dataproc.workflow_vars import WorkflowVariables


class WorkflowTemplate(PySparkJob, WorkflowVariables):
    """
    Class for interacting with the Cloud Dataproc API.
    """

    def __init__(self):
        PySparkJob.__init__(self)
        WorkflowVariables.__init__(self)

    def create_workflow_template(self, job_acquistion, job_ingestion, message):
        """
        Creates a workflow template.
        """
        step_name = self.get_step_id(job_acquistion)
        step_id_acquistion = f"captura-{step_name}"
        step_id_ingestion = f"ingestao-{step_name}"
        template = {
            "id": str(self.label_application),
            "labels": {
                "application": str(self.label_application),
                "billing-id": f"da-{step_name}",
                "cost-center": str(self.label_cost_center),
                "environment": str(self.label_environment),
                "product": str(self.label_application),
                "resource": "google_dataproc_cluster",
                "squad": str(self.label_squad),
                "type": "dataproc_cluster",
                "value-stream": str(self.label_value_stream),
            },
            "jobs": [
                {
                    "pyspark_job": {
                        "main_python_file_uri": job_acquistion,
                        "args": [
                            message,
                            self.bucket_name,
                            self.project_id,
                            self.tmp_dir,
                        ],
                        "python_file_uris": self.python_file_uris,
                    },
                    "step_id": step_id_acquistion,
                },
                {
                    "pyspark_job": {
                        "main_python_file_uri": job_ingestion,
                        "args": [
                            message,
                            step_name,
                            self.bucket_name,
                            self.dataset,
                            self.project_id,
                            self.project_name,
                            self.spark_app_name,
                            self.spark_master,
                            self.tmp_dir,
                            self.table_name_sancoes_ceis,
                            self.table_name_sancoes_cepim,
                            self.table_name_sancoes_cnep,
                            self.table_name_sancoes_acordos,
                            self.table_name_sancoes_efeitos,
                            self.table_name_ibama,
                            self.table_name_aux_brasil,
                            self.table_name_aux_emerg,
                            self.table_name_bf_pagamentos,
                            self.table_name_bf_saques,
                            self.table_name_bpc,
                            self.table_name_garantias_safra,
                            self.table_name_peti,
                            self.table_name_seguro_defeso,
                            self.table_name_trab_formal,
                            self.table_name_empr_domestico,
                            self.table_name_trab_resgatado,
                            self.table_name_bolsa_qualific,
                            self.table_name_pesc_artesanal,
                            self.table_name_aposentados,
                            self.table_name_honor_advoc,
                            self.table_name_honor_jetons,
                            self.table_name_militares,
                            self.table_name_pensionistas,
                            self.table_name_reserva_ref_militares,
                            self.table_name_servidores,
                            self.table_name_servidores_sp,
                            self.table_name_servidores_mg,
                            self.table_name_servidores_es,
                            self.table_name_fornecedores_compras,
                            self.table_name_fornecedores_item_compra,
                            self.table_name_fornecedores_termo_aditivo,
                            self.table_name_cnpj_cnaes,
                            self.table_name_cnpj_empresas,
                            self.table_name_cnpj_estabelecimentos,
                            self.table_name_cnpj_imune_isentas,
                            self.table_name_cnpj_lucro_arbitrado,
                            self.table_name_cnpj_lucro_presumido,
                            self.table_name_cnpj_lucro_real,
                            self.table_name_cnpj_motivos,
                            self.table_name_cnpj_municipios,
                            self.table_name_cnpj_natureza_juridica,
                            self.table_name_cnpj_paises,
                            self.table_name_cnpj_qualificacao_socio,
                            self.table_name_cnpj_simples,
                            self.table_name_cnpj_socios,
                            self.table_name_servidores_sc,
                            self.table_name_debitos_prev,
                            self.table_name_debitos_nao_prev,
                            self.table_name_debitos_fgts,
                            self.table_name_servidores_pr,
                            self.table_name_novo_bf,
                            self.table_name_servidores_df_desp_emp,
                            self.table_name_servidores_df_desp_emp_desc,
                            self.table_name_servidores_df_desp_emp_sub,
                            self.table_name_servidores_df_desp_lanc,
                            self.table_name_servidores_df_desp_nt_lanc,
                            self.table_name_servidores_df_desp_nt_lanc_evento,
                            self.table_name_servidores_df_desp_ord_canc,
                            self.table_name_servidores_df_desp_ord_emp,
                            self.table_name_servidores_df_desp_pgto,
                            self.table_name_servidores_df_desp_principal,
                            self.table_name_servidores_df_licitacao,
                            self.table_name_servidores_df_orgao,
                            self.table_name_servidores_df_patrimonio,
                            self.table_name_servidores_df_receita,
                            self.table_name_servidores_df_remuneracao,
                            self.table_name_servidores_df_remuneracao_det,
                        ],
                        "python_file_uris": self.python_file_uris,
                        "jar_file_uris": [
                            job_ingestion,
                            "gs://spark-lib/bigquery/spark-bigquery-latest.jar",
                        ],
                        "logging_config": {"driver_log_levels": {"root": "ERROR"}},
                    },
                    "step_id": step_id_ingestion,
                    "prerequisite_step_ids": [step_id_acquistion],
                },
            ],
            "placement": {
                "managed_cluster": {
                    "cluster_name": f"da-{step_name}",
                    "config": {
                        "config_bucket": self.bucket_jobs_name,
                        "gce_cluster_config": {
                            "metadata": {
                                "PIP_PACKAGES": "pandas==2.1.0 beautifulsoup4==4.12.1 unrar==0.4 rarfile==4.0 requests==2.24.0 google-api-core==1.23.0 google-api-python-client==1.10.0 google-auth==1.23.0 google-auth-httplib2==0.0.4 google-cloud-core==1.4.3 google-cloud-trace==0.23.0 googleapis-common-protos==1.52.0 google-cloud-storage==1.31.2 google-cloud-pubsub==2.1.0 google-cloud-bigquery==2.6.1"
                            },
                            "zone_uri": "us-east1-b",
                            "service_account": self.service_account,
                            "service_account_scopes": [
                                "https://www.googleapis.com/auth/cloud-platform"
                            ],
                            "subnetwork_uri": self.subnetwork,
                            "internal_ip_only": True,
                        },
                        "initialization_actions": [
                            {
                                "executable_file": f"gs://{self.bucket_init_actions_name}/goog-dataproc-initialization-actions-us-east1/python/pip-install.sh",
                                "execution_timeout": "500s",
                            }
                        ],
                        "master_config": {
                            "num_instances": int(self.master_num_instances),
                            "machine_type_uri": self.machine_type_uri,
                        },
                        "worker_config": {
                            "num_instances": int(self.worker_num_instances),
                            "machine_type_uri": self.machine_type_uri,
                            "disk_config": {
                                "boot_disk_type": "pd-standard",
                                "boot_disk_size_gb": int(self.disk_size_gb),
                                "num_local_ssds": 0,
                                "local_ssd_interface": "SCSI",
                            },
                        },
                        "software_config": {
                            "image_version": "2.1.20-debian11",
                            "properties": {
                                "dataproc:dataproc.allow.zero.workers": "true"
                            },
                        },
                    },
                },
            },
        }
        return template
