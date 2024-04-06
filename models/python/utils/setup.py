# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: setup.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: Setup script for Profissionais Liberais.
# ================================================================================================
from __future__ import annotations

import os

import setuptools

package_version = os.environ.get("APP_VERSION", "1.0.0")
app_artifact_id = os.environ.get("APP_ARTIFACT_ID", "")

package_root = os.path.abspath(os.path.dirname(__file__))

readme_filename = os.path.join(package_root, "README.md")
with open(readme_filename, encoding="utf-8") as readme_file:
    readme = readme_file.read()

setuptools.setup(
    name=app_artifact_id,
    version=package_version,
    author="",
    author_email="",
    description="",
    long_description=readme,
    long_description_content_type="",
    packages=setuptools.find_packages(),
    python_requires=">=3.10",
    keywords="",
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
)
