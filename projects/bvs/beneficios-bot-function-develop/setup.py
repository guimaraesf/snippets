#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ================================================================================================
# Module: setup.py
# Author: Fernando Theodoro Guimarães
# E-mail: fernando.guimaraes@boavistascpc.com.br
# Description: This code is ...
# Value Stream: Data
# Squad: Dados Alternativos
# ================================================================================================


import os
import setuptools

package_version = os.environ.get("APP_VERSION", "1.0.0")
app_artifact_id = os.environ.get("APP_ARTIFACT_ID", "beneficios-bot")

setuptools.setup(
    name=app_artifact_id,
    version=package_version,
    author="Dados-Alternativos",
    description="Automação para capturar dados públicos",
    packages=setuptools.find_packages(),
    python_requires=">=3.9",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
