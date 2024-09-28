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


class PySparkJob:
    """
    Class for create PySpark Jobs.
    """

    def __init__(self):
        self.jobs = []

    def get_step_id(self, id):
        """
        Returns the id of the job.
        """
        start, end = (13, -3)
        step_id = str(id.split("/")[-1][start:end].replace("_", "-"))
        return step_id
