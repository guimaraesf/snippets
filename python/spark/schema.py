#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ================================================================================================
# Module: schema.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This code is responsible for creating the schemas of each table.
# ================================================================================================
from pyspark.sql.types import StructField, StructType
from pyspark.sql.types import StructType


def create_schema(schema_atrr: dict) -> StructType:
	schema = StructType()
	for column_name, fields in schema_atrr.items():
		data_type, nullable, metadata = fields
		field = StructField(column_name, data_type, nullable, metadata)
		schema.add(field)
	return schema
