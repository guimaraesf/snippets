# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: set_variables.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This DAG is setting environment variables in Airflow
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
# DAG ARGS
# ================================================================================================

DEFAULT_ARGS = {
    "owner": "",
    "depends_on_past": False,
    "description": "",
    "retries": 2,
    "retry_delay": "",
    "task_id": "",
    "start_date": "",
}


# ================================================================================================
# DAG INPUTS
# ================================================================================================
def get_json_config(self):
    """
    Gets the pipeline configuration from the file.

    Returns:
        str: A string representation of the pipeline configuration.
    """
    with open(self.souce_path, "r", encoding="UTF-8") as f:
        data = json.load(f)
        return json.dumps(data, skipkeys=True, indent=4)


DAG_ID = ""
SCHEDULE_INTERVAL = "@daily"
START_DATE = days_ago(0)
RETRY_DELAY = timedelta(seconds=30)
PATH = f"/home/airflow/gcs/dags/{{}}/variables.json"
JSON_CONFIG = get_json_config()

# ================================================================================================
# DAG TASKS
# ================================================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
    doc_md=__DOC__,
) as dag:
    BashOperator(
        task_id="set_variables",
        bash_command=f"airflow variables set VARIABLE_NAME '{JSON_CONFIG}'",
        doc_md=__DOC__,
        dag=dag,
    )
