#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: main.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code is triggered by the cloud function to start the data capture process
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import sys
import base64
import binascii

# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../../"))
from src.utils.gcp.dataproc.workflow_submit_jobs import SubmitJobs
from src.utils.config.variables import Variables
from src.utils.config.app_log import AppLogging


logger = AppLogging()
submit_jobs = SubmitJobs()


def run_jobs(event, context):
    """
    Triggered by a message to a Cloud PubSub Message.
    """
    try:
        BUCKET_JOBS_ID: str = os.getenv("BUCKET_JOBS_NAME")
        pubsub_message: str = base64.b64decode(event["data"]).decode("utf-8")

        job_values = Variables.JOBS.values()
        if pubsub_message in ["monthly-task", "daily-task"]:
            if pubsub_message == "monthly-task":
                job_names = [
                    job_name
                    for job_name in job_values
                    if job_name not in ["sancoes", "ibama"]
                ]
            else:
                job_names = [
                    job_name
                    for job_name in job_values
                    if job_name in ["sancoes", "ibama"]
                ]

            jobs_to_submit: list = [
                f"gs://{BUCKET_JOBS_ID}/src/data/acquisition/tasks/main_cronjob_{job_name}.py"
                for job_name in job_names
            ]
            submit_jobs.run(jobs_to_submit, pubsub_message)
    except (KeyError, TypeError) as error:
        logger.error(f"Error accessing job variables: {error}")
    except (binascii.Error, UnicodeDecodeError) as error:
        logger.error(f"Error decoding Pub/Sub message: {error}")
