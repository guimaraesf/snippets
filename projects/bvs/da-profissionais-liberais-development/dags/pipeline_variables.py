# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: pipeline_config.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This DAG is setting environment variables in Airflow
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
import datetime
import json
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# ================================================================================================
# DAG DOCS
# ================================================================================================
__DOC__ = """
Esta DAG é responsável por configurar o ambiente para a execução de outros DAGs.\n
Ele importa as variáveis necessárias para a execução de outros DAGs.
"""


# ================================================================================================
# DAG BUILD
# ================================================================================================
class DagVariables:
    def __init__(self, souce_path: str) -> None:
        """
        Constructor for the DagConfig class.

        Args:
            souce_path (str): The path to the pipeline configuration file.
        """
        self.souce_path = souce_path

    def get_json_config(self):
        """
        Gets the pipeline configuration from the file.

        Returns:
            str: A string representation of the pipeline configuration.
        """
        with open(self.souce_path, "r", encoding="UTF-8") as f:
            data = json.load(f)
            return json.dumps(data, skipkeys=True, indent=4)


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


class DagPipeline:
    def __init__(
        self,
        dag_id: str,
        default_args: dict,
        doc_md: str,
        schedule_interval: str,
        json_config: dict,
    ) -> None:
        """
        Constructor for the DagPipeline class.

        Args:
            dag_id (str): The ID of the Directed Acyclic Graph (DAG).
            default_args (dict): The default arguments for the DAG.
            doc_md (str): The markdown documentation for the DAG.
            schedule_interval (str): The schedule interval of the DAG.
            json_config (dict): The configuration for the pipeline.
        """
        self.dag_id = dag_id
        self.default_args = default_args
        self.doc_md = doc_md
        self.schedule_interval = schedule_interval
        self.json_config = json_config

    def create_dag(self) -> None:
        """
        Runs the DAG with the given configuration.
        """
        with DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            doc_md=self.doc_md,
        ) as dag:
            BashOperator(
                task_id="set_pipeline_variables",
                bash_command=f"airflow variables set PROFISSIONAIS_LIBERAIS_CONFIG '{self.json_config}'",
                doc_md=self.doc_md,
                dag=dag,
            )


# ================================================================================================
# DAG INPUTS
# ================================================================================================
SQUAD = "dados-alternativos"
DAG_ID = "da_profissionais_liberais_pipeline_variables"
SCHEDULE_INTERVAL = "@daily"
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

dag_variables = DagVariables(
    "/home/airflow/gcs/dags/dependencies/dados_alternativos/profissionais_liberais/config/variables.json"
)
JSON_CONFIG = dag_variables.get_json_config()

# ================================================================================================
# DAG CREATE
# ================================================================================================

dag_pipeline = DagPipeline(
    DAG_ID, DEFAULT_ARGS, __DOC__, SCHEDULE_INTERVAL, JSON_CONFIG
)
dag_pipeline.create_dag()

globals()[dag_pipeline.dag_id] = dag_pipeline
