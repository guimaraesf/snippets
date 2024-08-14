# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import os
import sys

sys.path.insert(0, os.path.abspath(".."))
sys.path.insert(0, os.path.abspath(""))

__version__ = ""

# -- Project information -----------------------------------------------------

project = "beneficios-bot"
copyright = "2023, Fernando Theodoro Guimarães"
author = "Fernando Theodoro Guimarães"
release = "1.0.0"


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.coverage",
    "sphinx.ext.napoleon",
    "sphinx.ext.duration",
    "sphinx_rtd_theme"
]

templates_path = ["_templates"]
source_suffix = [".rst", ".md"]
exclude_patterns = [
    "setup.py"
    "Thumbs.db",
    "DS_Store",
    "library/xml.rst",
    "library/xml",
    "library/xml*",
    "**/.svn",
]

# The full version, including alpha/beta/rc tags.
release = __version__
# The short X.Y version.
version = ".".join(release.split(".")[0:2])

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
html_title = "Beneficios-bot documentation"
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_last_updated_fmt = "%a, %d %b %Y %H:%M:%S"
language = "pt-BR"

# -- Options for sphinx.ext.autodoc -------------------------------------------------
autodoc_member_order = "alphabetical"
autodoc_typehints = "True"
suppress_warnings = ["True"]
autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'show-inheritance': True,
}

def skip_modules(app, what, name, obj, skip, options):
    """
    Skip the given module and all its dependencies
    """
    modules = [
        "pandas", "zipfile", "unidecode", 
        "google", "pyspark", "pytz"
    ]
    if name in modules:
        return True
    return skip


def setup(app):
    """
    Setup the sphinx extension
    """
    app.connect("autodoc-skip-member", skip_modules)
