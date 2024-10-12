import unittest

from pyspark.sql.types import (
	StructType,
	StringType,
	IntegerType,
	BooleanType,
)

from python.spark.schema import create_schema


class TestCreateSchema(unittest.TestCase):
	
	def test_create_empty_schema(self):
		# Arrange
		schema_atrr = {}
		
		# Act
		schema = create_schema(schema_atrr)
		
		# Assert
		self.assertIsInstance(schema, StructType)
		self.assertEqual(len(schema.fields), 0)
	
	
	def test_single_column_schema(self):
		# Arrange
		schema_atrr = {"col1": (StringType(), True, {"description": "A string column"})}

		# Act
		schema = create_schema(schema_atrr)
		
		# Assert
		self.assertIsInstance(schema, StructType)
		self.assertEqual(len(schema.fields), 1)
		self.assertEqual(schema.fields[0].name, "col1")
		self.assertIsInstance(schema.fields[0].dataType, StringType)
		self.assertTrue(schema.fields[0].nullable)
		self.assertEqual(schema.fields[0].metadata["description"], "A string column")
	
	
	def test_multiple_columns_schema(self):
		# Arrange
		schema_atrr = {
			"col1": (StringType(), True, {"description": "A string column"}),
			"col2": (IntegerType(), False, {"description": "An integer column"}),
			"col3": (BooleanType(), True, {}),
		}
		
		# Act
		schema = create_schema(schema_atrr)
		
		# Assert
		self.assertIsInstance(schema, StructType)
		self.assertEqual(len(schema.fields), 3)


if __name__ == "__main__":
	unittest.main()
