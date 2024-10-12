#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# --------------------------------------------------------------------------------------------------
# Module Name: *.py
# Description:
# Author: Fernando Theodoro Guimarães
# --------------------------------------------------------------------------------------------------
from enum import Enum


class Example(Enum):
	"""
	Class to set all columns for schema on DataFrames.
	"""
	
	
	@classmethod
	def init(cls):
		return {var.name: var.value for var in cls}
	
	
	NOME = "FERNANDO THEODORO GUIMARÃES"
	IDADE = 27
