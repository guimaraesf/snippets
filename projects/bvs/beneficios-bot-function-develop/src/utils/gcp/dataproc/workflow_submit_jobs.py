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
from time import sleep
from google.cloud import dataproc_v1 as dataproc

# Add the parent directory to the Python path
sys.path.append(os.path.abspath("../"))
from src.utils.gcp.dataproc.workflow_vars import WorkflowVariables
from src.utils.gcp.dataproc.template import WorkflowTemplate


class SubmitJobs(WorkflowTemplate, WorkflowVariables):
    """
    Class for interacting with the Cloud Dataproc API.
    """

    def __init__(self):
        WorkflowTemplate.__init__(self)
        WorkflowVariables.__init__(self)
        self.parent = f"projects/{self.project_id}/regions/{self.region}"
        self.client_options = {
            "api_endpoint": f"{self.region}-dataproc.googleapis.com:443"
        }
        self.request = {
            "project_id": self.project_id,
            "region": self.region,
            "cluster_name": self.cluster_name,
        }

    def instantiate_workflow_client(self):
        """
        Instantiates a client with the endpoint set to the desired region.
        """
        workflow_template_client = dataproc.WorkflowTemplateServiceClient(
            client_options=self.client_options
        )
        return workflow_template_client

    def instantiate_workflow(self, job_acquistion, job_ingestion, message):
        """
        Submits a workflow for a Cloud Dataproc using the Python client library.
        """
        # Create a client with the endpoint set to the desired region.
        workflow_template_client = self.instantiate_workflow_client()
        template = self.create_workflow_template(job_acquistion, job_ingestion, message)
        workflow_template_client.instantiate_inline_workflow_template(
            request={"parent": self.parent, "template": template}
        )
        print(f"{'*' * 50} TASK STARTED {'*' * 50}")
        print(f"Running Python file on Cloud Storage URI: {job_acquistion}")

    def run(self, jobs, pubsub_message):
        """
        Processes a file based on a Cloud PubSub message
        """
        try:
            job_ingestion = self.python_file_uris[
                0
            ]  # "src/data/ingestion/ingestion.py"
            for job_acquistition in jobs:
                self.instantiate_workflow(
                    job_acquistition, job_ingestion, pubsub_message
                )
                print(f"{'*' * 50} TASK FINISHED {'*' * 50}")
                sleep(5)
        except (ValueError, KeyError) as error:
            print(f"Error: {error}")
