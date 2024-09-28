# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: setup.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: Setup script for Profissionais Liberais.
# ================================================================================================
from __future__ import annotations
import sys
import os

import setuptools

def _open(filename):
    if sys.version_info[0] == 2:
        return open(filename)
    return open(filename, encoding="utf-8")

package_version = os.environ.get("APP_VERSION", "1.0.0")
app_artifact_id = os.environ.get("APP_ARTIFACT_ID", "")

package_root = os.path.abspath(os.path.dirname(__file__))

# Getting description:
with _open("README.rst") as readme_file:
    description = readme_file.read()

# Getting requirements:
with _open("requirements.txt") as requirements_file:
    requirements = requirements_file.readlines()

setuptools.setup(
    name=app_artifact_id,
    version=package_version,
    author="",
    author_email="",
    description="",
    long_description=description,
    author="",
    author_email="",
    maintainer="",
    maintainer_email="",
    packages=setuptools.find_packages(),
    python_requires=">=3.10",
    install_requires=requirements,
    zip_safe=True,
    keywords=[],
    classifiers=[
        "Development Status :: ",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
)
