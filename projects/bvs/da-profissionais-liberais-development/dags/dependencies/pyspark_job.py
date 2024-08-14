# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: pyspark_job.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for setting the PySpark Job parameters
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
# Here is the directory structure for running in Cloud Composer
import re
from typing import Optional

from dependencies.dados_alternativos.profissionais_liberais.variables import Variables


class PySparkJob(Variables):
    """
    This class is used to configure a cluster.
    """

    def __init__(self, pipeline_config: dict) -> None:
        """
        Initializes the PySparkJob class.

        Args:
            pipeline_config (dict): The configuration for the pipeline.
        """
        self.pipeline_config = pipeline_config
        super().__init__(self.pipeline_config)

    def get_labels(self) -> dict:
        """
        Retrieves the labels for the cluster.

        Returns:
            dict: A dictionary containing the labels for the cluster.
        """
        return {
            "application": str(self.get_label_application),
            "billing-id": str(self.get_label_application),
            "environment": str(self.get_label_environment),
            "cost-center": str(self.get_label_cost_center),
            "product": str(self.get_label_application),
            "resource": "google-dataproc-cluster",
            "squad": str(self.get_label_squad),
            "type": "dataproc-cluster",
            "value-stream": str(self.get_label_value_stream),
        }

    @staticmethod
    def get_schema_job(
        main_python_file_uri: str,
        args: list,
        python_file_uris: list,
        jar_file_uris: Optional[list] = None,
        file_uris: Optional[list] = None,
        archive_uris: Optional[list] = None,
        properties: Optional[dict] = None,
    ) -> dict:
        """
        Retrieves the schema job for PySpark.

        Args:
            main_python_file_uri (str): The URI of the main Python file.
            args (list): The arguments for the job.
            python_file_uris (list): The URIs of the Python files.
            jar_file_uris (list, optional): The URIs of the JAR files. Defaults to None.
            file_uris (list, optional): The URIs of the files. Defaults to None.
            archive_uris (list, optional): The URIs of the archives. Defaults to None.
            properties (dict, optional): The properties for the job. Defaults to None.

        Returns:
            dict: The schema job for PySpark.
        """
        pyspark_job = {
            "main_python_file_uri": main_python_file_uri,
            "args": args,
            "python_file_uris": python_file_uris,
            "jar_file_uris": jar_file_uris,
            "file_uris": file_uris,
            "archive_uris": archive_uris,
            "properties": properties,
            "logging_config": {
                "driver_log_levels": {
                    "root": "ERROR",
                    "com.google": "FATAL",
                    "org.apache": "ERROR",
                },
            },
        }
        return pyspark_job

    def get_args(self, step_id: str) -> list:
        """
        Retrieves the arguments for the job.

        Args:
            step_id (str): The ID of the step.

        Returns:
            list: The arguments for the job.
        """
        return [
            step_id,
            self.get_bucket_id,
            self.get_cluster_id,
            self.get_dataset_id,
            self.get_project_id,
            self.get_spark_app_name,
            self.get_spark_master,
            self.get_port,
            self.get_tmp_dir,
        ]

    @staticmethod
    def validate_id(id: str) -> bool:
        pattern = r"^[a-zA-Z0-9_-]{1,100}$"
        if re.match(pattern, id):
            return id
        raise re.error("Invalid job id.", pattern)

    def create_job(
        self,
        job_id: str,
        cluster_name: str,
        main_python_file_uri: str,
        args: list,
        python_file_uris: list,
        jar_file_uris: Optional[list] = None,
        file_uris: Optional[list] = None,
        archive_uris: Optional[list] = None,
        properties: Optional[dict] = None,
    ) -> dict:
        """
        Creates a job.

        Args:
            job_id (str): The ID of the job.
            cluster_name (str): The name of the cluster.
            main_python_file_uri (str): The URI of the main Python file.
            args (list): The arguments for the job.
            python_file_uris (list): The URIs of the Python files.
            jar_file_uris (list, optional): The URIs of the JAR files. Defaults to None.
            file_uris (list, optional): The URIs of the files. Defaults to None.
            archive_uris (list, optional): The URIs of the archives. Defaults to None.
            properties (dict, optional): The properties for the job. Defaults to None.

        Returns:
            dict: The created job.
        """
        job = {
            "labels": self.get_labels(),
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": self.get_schema_job(
                main_python_file_uri,
                args,
                python_file_uris,
                jar_file_uris if jar_file_uris else [],
                file_uris if file_uris else [],
                archive_uris if archive_uris else [],
                properties if properties else {},
            ),
            "reference": {"project_id": self.get_project_id, "job_id": self.validate_id(job_id)},
        }
        return job
