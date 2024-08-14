# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: bt_client.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This module is responsible for handling objects in Google Cloud BigTable
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================
from google.cloud import bigtable


class BigTableClient:
    """
    A client for interacting with Google Cloud Bigtable.

    Attributes:
        project_id (str): The ID of the Google Cloud project.
        instance_id (str): The ID of the Bigtable instance.
    """

    def __init__(self, project_id: str, instance_id: str) -> None:
        """
        Initializes the BigTableClient with the given project and instance IDs.

        Args:
            project_id (str): The ID of the Google Cloud project.
            instance_id (str): The ID of the Bigtable instance.
        """
        self.project_id = project_id
        self.instance_id = instance_id

    def instantiate_client(self) -> bigtable.Client:
        """
        Instantiates a Bigtable client.

        Returns:
            bigtable.Client: A Bigtable client.
        """
        return bigtable.Client(project=self.project_id, admin=True)

    def get_bigtable_instance(self, client):
        """
        Gets a Bigtable instance.

        Args:
            client (bigtable.Client): The Bigtable client.

        Returns:
            bigtable.Instance: A Bigtable instance.
        """
        return client.instance(self.instance_id)

    def get_table_by_instance(self, instance, table_id: str):
        """
        Gets a table by its ID from a Bigtable instance.

        Args:
            instance (bigtable.Instance): The Bigtable instance.
            table_id (str): The ID of the table.

        Returns:
            bigtable.Table: The Bigtable table.
        """
        return instance.table(table_id)
