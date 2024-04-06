# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: utils.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for supporting the construction of the DAG
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import uuid
from typing import NamedTuple

from dependencies.dados_alternativos.profissionais_liberais.variables import Variables
from google.cloud import storage

# Here is the directory structure for running in Cloud Composer


class JobParams(NamedTuple):
    """
    JobParams NamedTuple

    This NamedTuple is used to store job information.

    Args:
        step (str): The step name for the job.
        job_id (str): The unique identifier for the job.
        python_file_uris (list): The list of URIs of the Python files.
        main_python_file_uri_extract (str): The URI of the main extract Python file for the job.
        main_python_file_uri_transform (str): The URI of the main transform Python file for the job.
        main_python_file_uri_load (str): The URI of the main load Python file for the job.
    """

    step: str
    job_id: str
    python_file_uris: list
    main_python_file_uri_extract: str
    main_python_file_uri_transform: str
    main_python_file_uri_load: str


class Utils(Variables):
    """
    This class is used to helper a DAG.
    """

    def __init__(self, pipeline_config: dict) -> None:
        """
        Initializes the Utils class.

        Args:
            pipeline_config (dict): The configuration for the pipeline.
        """
        self.pipeline_config = pipeline_config
        self.id = uuid.uuid4()
        super().__init__(self.pipeline_config)
        self.bucket_path = f"gs://{self.get_bucket_id}"
        self.main_python_file_uri_extract = f"{self.bucket_path}/scripts/src/pipeline/extract"
        self.main_python_file_uri_transform = (
            f"{self.bucket_path}/scripts/src/pipeline/transform/transformer.py"
        )
        self.main_python_file_uri_load = f"{self.bucket_path}/scripts/src/pipeline/load/loader.py"

    def get_bucket_obj(self):
        """
        Gets the bucket object from the storage client.

        Returns:
            The bucket object.
        """
        client = storage.Client(self.get_project_id)
        bucket = client.bucket(self.get_bucket_id)
        return bucket

    @staticmethod
    def _ends_name_is_py(blob: storage.Blob) -> bool:
        """
        Checks if the blob name starts with "scripts" and ends with ".py", and does not end with "__init__.py".

        Args:
            blob (storage.Blob): The blob to check.

        Returns:
            bool: True if the conditions are met, False otherwise.
        """
        return (
            blob.name.startswith("scripts")
            and blob.name.endswith(".py")
            and not blob.name.endswith("__init__.py")
        )

    def get_python_file_uris(self, blob_list: list) -> list:
        """
        Gets the URIs of the Python files in the blob list.

        Args:
            blob_list (list): The list of blobs.

        Returns:
            list: The list of URIs of the Python files.
        """
        return [
            f"{self.bucket_path}/{blob.name}" for blob in blob_list if self._ends_name_is_py(blob)
        ]

    def get_list_blobs(self, prefix: str = None) -> list:
        """
        Gets a list of blobs in the bucket that match the given prefix.

        Args:
            prefix (str, optional): The prefix to match. Defaults to None.

        Returns:
            list: A list of blobs that match the prefix.
        """
        bucket = self.get_bucket_obj()
        return list(bucket.list_blobs(prefix=prefix))

    @staticmethod
    def customize_cluster_name(original_string: str, insert_string: str, delimiter: str) -> str:
        """
        Inserts a string into the original string before the last occurrence of the delimiter.

        Args:
            original_string (str): The original string where the insertion will be made.
            insert_string (str): The string to be inserted into the original string.
            delimiter (str): The delimiter used to split the original string.

        Returns:
            str: The modified string with the inserted string.
        """
        parts = original_string.split(delimiter)
        parts.insert(-1, insert_string)
        return delimiter.join(parts)

    def task_parameters_map(self) -> dict:
        """
        Gets the tasks for the pipeline.

        Returns:
            dict: The tasks for the pipeline.
        """
        BLOBS = self.get_list_blobs(prefix="scripts/src")
        PYTHON_FILE_URIS = self.get_python_file_uris(BLOBS)
        return {
            "conselho-federal-contabilidade": JobParams(
                step="{}",
                job_id=f"""cfc-{{}}-{self.id}""",
                python_file_uris=PYTHON_FILE_URIS,
                main_python_file_uri_extract=f"{self.main_python_file_uri_extract}/conselho_federal_contabilidade.py",
                main_python_file_uri_transform=self.main_python_file_uri_transform,
                main_python_file_uri_load=self.main_python_file_uri_load,
            ),
            "superintendencia-seguros-privados": JobParams(
                step="{}",
                job_id=f"""susep-{{}}-{self.id}""",
                python_file_uris=PYTHON_FILE_URIS,
                main_python_file_uri_extract=f"{self.main_python_file_uri_extract}/superintendencia_seguros_privados.py",
                main_python_file_uri_transform=self.main_python_file_uri_transform,
                main_python_file_uri_load=self.main_python_file_uri_load,
            ),
            "conselho-federal-educacao-fisica": JobParams(
                step="{}",
                job_id=f"""confef-{{}}-{self.id}""",
                python_file_uris=PYTHON_FILE_URIS,
                main_python_file_uri_extract=f"{self.main_python_file_uri_extract}/conselho_federal_educacao_fisica.py",
                main_python_file_uri_transform=self.main_python_file_uri_transform,
                main_python_file_uri_load=self.main_python_file_uri_load,
            ),
            "conselho-federal-fonoaudiologia": JobParams(
                step="{}",
                job_id=f"""cffa-{{}}-{self.id}""",
                python_file_uris=PYTHON_FILE_URIS,
                main_python_file_uri_extract=f"{self.main_python_file_uri_extract}/conselho_federal_fonoaudiologia.py",
                main_python_file_uri_transform=self.main_python_file_uri_transform,
                main_python_file_uri_load=self.main_python_file_uri_load,
            ),
        }
