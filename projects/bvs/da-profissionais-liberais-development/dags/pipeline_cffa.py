# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: pipeline_cffa.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for orchestrating the Conselho Federal de Fonoaudilogia pipeline.
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import datetime
import json
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from dependencies.dados_alternativos.profissionais_liberais.cluster_config import (
    ClusterConfig,
)
from dependencies.dados_alternativos.profissionais_liberais.pyspark_job import (
    PySparkJob,
)
from dependencies.dados_alternativos.profissionais_liberais.utils import Utils
from dependencies.dados_alternativos.profissionais_liberais.variables import Variables

# Here is the directory structure for running in Cloud Composer

# ================================================================================================
# DAG DOCS
# ================================================================================================

__DOC__ = """
1. Início do Pipeline: O pipeline começa com um `DummyOperator` chamado start_pipeline.\n

2. Criação do Cluster: Em seguida, um cluster Dataproc é criado usando o operador `DataprocCreateClusterOperator`. O
cluster é configurado com os parâmetros especificados e é deletado em caso de erro.\n

3. Execução do Pipeline: Uma vez que o cluster esteja pronto, o pipeline de processamento de dados é executado. Este
pipeline é um grupo de tarefas chamado `pipeline`, que contém três etapas: `extract-cffa`, `transform-cffa` e
`load-cffa`. Cada etapa é um `PySpark Job` submetido ao cluster Dataproc usando o operador
`DataprocSubmitJobOperator`. As etapas são executadas em sequência, ou seja, a etapa de transformação só começa após
a conclusão da etapa de extração, e a etapa de carregamento só começa após a conclusão da etapa de transformação.\n

4. Deleção de Objetos GCS: Após a conclusão do pipeline, os objetos no Google Cloud Storage (GCS) são deletados
usando o operador `GCSDeleteObjectsOperator`. Os objetos a serem deletados são especificados por um prefixo.\n

5. Deleção do Cluster: O cluster Dataproc é então deletado usando o operador DataprocDeleteClusterOperator.\n

6. Fim do Pipeline: Finalmente, o pipeline é concluído com outro `DummyOperator` chamado stop_pipeline.\n
"""


# ================================================================================================
# DAG BUILD
# ================================================================================================
class DagConfig:
    def __init__(
        self,
        owner: str,
        depends_on_past: bool,
        description: str,
        retries: int,
        retry_delay: timedelta,
        start_date: datetime,
        task_id: str,
    ):
        """
        Constructor for the DagConfig class.

        Args:
            owner (str): The owner of the DAG.
            depends_on_past (bool): Whether the DAG depends on past runs.
            description (str): The description of the DAG.
            retries (int): The number of retries for the DAG.
            retry_delay (timedelta): The delay between retries.
            start_date (datetime): The start date of the DAG.
            task_id (str): The ID of the task.
        """
        self.owner = owner
        self.depends_on_past = depends_on_past
        self.description = description
        self.retries = retries
        self.retry_delay = retry_delay
        self.task_id = task_id
        self.start_date = start_date

    def get_args(self) -> dict:
        """
        Gets the arguments of the DAG configuration.

        Returns:
            dict: A dictionary containing the arguments of the DAG configuration.
        """
        return {
            "owner": self.owner,
            "depends_on_past": self.depends_on_past,
            "description": self.description,
            "retries": self.retries,
            "retry_delay": self.retry_delay,
            "task_id": self.task_id,
            "start_date": self.start_date,
        }


class DagPipeline(Variables):
    def __init__(
        self,
        dag_id: str,
        default_args: dict,
        doc_md: str,
        schedule_interval: str,
        pipeline_config: dict,
        cluster_config=ClusterConfig,
        pyspark_job=PySparkJob,
        utils=Utils,
    ) -> None:
        """
        Constructor for the DagPipeline class.

        Args:
            dag_id (str): The ID of the Directed Acyclic Graph (DAG).
            default_args (dict): The default arguments for the DAG.
            doc_md (str): The markdown documentation for the DAG.
            schedule_interval (str): The schedule interval of the DAG.
            pipeline_config (dict): The configuration for the pipeline.
        """
        self.dag_id = dag_id
        self.default_args = default_args
        self.doc_md = doc_md
        self.schedule_interval = schedule_interval
        self.pipeline_config = pipeline_config
        self.cluster_config = cluster_config(self.pipeline_config)
        self.pyspark_job = pyspark_job(self.pipeline_config)
        self.utils = utils(self.pipeline_config)
        super().__init__(self.pipeline_config)
        self.cluster_name = self.utils.customize_cluster_name(
            self.get_cluster_id, "cffa", "-"
        )

    @staticmethod
    def create_empty_operator(task_id: str, dag: DAG) -> DummyOperator:
        """
        Creates an empty operator in the DAG.

        Args:
            task_id (str): The ID of the task.
            dag (DAG): The DAG object.

        Returns:
            DummyOperator: An instance of the DummyOperator class.
        """
        return DummyOperator(task_id=task_id, dag=dag)

    def create_cluster(self, task_id: str, dag: DAG) -> DataprocCreateClusterOperator:
        """
        Creates a Dataproc cluster.

        Args:
            task_id (str): The ID of the task.
            dag (DAG): The DAG object.

        Returns:
            DataprocCreateClusterOperator: An instance of the DataprocCreateClusterOperator class.
        """
        return DataprocCreateClusterOperator(
            task_id=task_id,
            project_id=self.get_project_id,
            cluster_name=self.cluster_name,
            labels=self.cluster_config.get_labels(),
            cluster_config=self.cluster_config.get_cluster_config(self.cluster_name),
            region=self.get_region,
            delete_on_error=True,
            dag=dag,
        )

    def submit_job(
        self,
        task_id: str,
        job_id: str,
        main_python_file_uri: str,
        args: list,
        python_file_uris: list,
        dag: DAG,
    ) -> DataprocSubmitJobOperator:
        """
        Submits a job to the Dataproc cluster.

        Args:
            task_id (str): The ID of the task.
            job_id (str): The ID of the job.
            main_python_file_uri (str): The URI of the main extract Python file for the job.
            args (list) : The list of strings for arguments in PySpark Job.
            python_file_uris (list) : The list of URIs of the Python files.
            dag (DAG): The DAG object.

        Returns:
            DataprocSubmitJobOperator: An instance of the DataprocSubmitJobOperator class.
        """

        return DataprocSubmitJobOperator(
            task_id=task_id,
            project_id=self.get_project_id,
            region=self.get_region,
            job=self.pyspark_job.create_job(
                job_id=job_id,
                cluster_name=self.cluster_name,
                main_python_file_uri=main_python_file_uri,
                args=args,
                python_file_uris=python_file_uris,
            ),
            dag=dag,
        )

    def create_pipeline(self, group_id: str, tooltip: str, dag: DAG):
        """
        Creates a pipeline of tasks within a TaskGroup.

        Args:
            group_id (str): The ID of the group.
            tooltip (str): The tooltip for the TaskGroup.
            dag (DAG): The DAG object.

        Returns:
            TaskGroup: An instance of the TaskGroup class.
        """
        JOB_PARAMS = self.utils.task_parameters_map().get(tooltip)
        STEP_EXTRACT, STEP_TRANSFORM, STEP_LOAD = [
            JOB_PARAMS.step.format(step)
            for step in ("extract-cffa", "transform-cffa", "load-cffa")
        ]

        with TaskGroup(group_id=group_id, tooltip=tooltip, dag=dag) as pipeline:
            extract_job = self.submit_job(
                JOB_PARAMS.step.format(STEP_EXTRACT),
                JOB_PARAMS.job_id.format(STEP_EXTRACT.split("-")[0]),
                JOB_PARAMS.main_python_file_uri_extract,
                self.pyspark_job.get_args(STEP_EXTRACT),
                JOB_PARAMS.python_file_uris,
                dag,
            )

            transform_job = self.submit_job(
                JOB_PARAMS.step.format(STEP_TRANSFORM),
                JOB_PARAMS.job_id.format(STEP_TRANSFORM.split("-")[0]),
                JOB_PARAMS.main_python_file_uri_transform,
                self.pyspark_job.get_args(STEP_TRANSFORM),
                JOB_PARAMS.python_file_uris,
                dag,
            )

            load_job = self.submit_job(
                JOB_PARAMS.step.format(STEP_LOAD),
                JOB_PARAMS.job_id.format(STEP_LOAD.split("-")[0]),
                JOB_PARAMS.main_python_file_uri_load,
                self.pyspark_job.get_args(STEP_LOAD),
                JOB_PARAMS.python_file_uris,
                dag,
            )
            (extract_job >> transform_job >> load_job)

        return pipeline

    def delete_gcs_objects(
        self, task_id: str, prefix: list, dag: DAG
    ) -> GCSDeleteObjectsOperator:
        """
        Creates an operator to delete objects in Google Cloud Storage.

        Args:
            task_id (str): The ID of the task.
            prefix (str): The prefix of the objects to be deleted.
            dag (DAG): The DAG object.

        Returns:
            GCSDeleteObjectsOperator: An instance of the GCSDeleteObjectsOperator class.
        """
        return GCSDeleteObjectsOperator(
            task_id=task_id,
            bucket_name=self.get_bucket_id,
            objects=None,
            prefix=prefix,
            dag=dag,
        )

    def delete_cluster(self, task_id: str, dag: DAG) -> DataprocDeleteClusterOperator:
        """
        Creates an operator to delete a Dataproc cluster.

        Args:
            task_id (str): The ID of the task.
            dag (DAG): The DAG object.

        Returns:
            DataprocDeleteClusterOperator: An instance of the DataprocDeleteClusterOperator class.
        """
        return DataprocDeleteClusterOperator(
            task_id=task_id,
            region=self.get_region,
            cluster_name=self.cluster_name,
            project_id=self.get_project_id,
            dag=dag,
        )

    def create_dag(self) -> DAG:
        """
        Creates a DAG.
        """
        with DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            doc_md=self.doc_md,
            schedule_interval=self.schedule_interval,
        ) as dag:
            start_pipeline = self.create_empty_operator("start", dag)
            create_cluster = self.create_cluster("create_dataproc_cluster", dag)
            pipeline = self.create_pipeline(
                "pipeline", "conselho-federal-fonoaudiologia", dag
            )
            delete_gcs_objects = self.delete_gcs_objects(
                "delete_files",
                ["transient/conselho-federal-fonoaudiologia"],
                dag,
            )
            delete_cluster = self.delete_cluster("delete_dataproc_cluster", dag)
            stop_pipeline = self.create_empty_operator("stop", dag)
            (
                start_pipeline
                >> create_cluster
                >> pipeline
                >> delete_gcs_objects
                >> delete_cluster
                >> stop_pipeline
            )


# ================================================================================================
# DAG INPUTS
# ================================================================================================
SQUAD = "dados-alternativos"
DAG_ID = "da_profissionais_liberais_pipeline_cffa"
AIRFLOW_VARIABELS = Variable.get("PROFISSIONAIS_LIBERAIS_CONFIG")
PIPELINE_CONFIG = json.loads(AIRFLOW_VARIABELS)
SCHEDULE_INTERVAL = "@monthly"
START_DATE = days_ago(0)
RETRY_DELAY = timedelta(seconds=30)

dag_config = DagConfig(
    owner=SQUAD,
    depends_on_past=False,
    description=__DOC__,
    retries=0,
    retry_delay=RETRY_DELAY,
    task_id=DAG_ID,
    start_date=START_DATE,
)

DEFAULT_ARGS = dag_config.get_args()

# ================================================================================================
# DAG CREATE
# ================================================================================================
dag_pipeline = DagPipeline(
    DAG_ID, DEFAULT_ARGS, __DOC__, SCHEDULE_INTERVAL, PIPELINE_CONFIG
)
dag_pipeline.create_dag()

globals()[dag_pipeline.dag_id] = dag_pipeline
