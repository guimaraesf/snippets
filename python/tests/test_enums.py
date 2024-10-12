import unittest
from python.utils.enums import Example


class TestEnums(unittest.TestCase):
	@classmethod
	def setUp(cls):
		cls.EnumExemple = ""
	
	
	def test_enum_members(self):
		"""
		Tests that expected members are present in the Columns enum.
		"""
		# Arrange
		expected_members = ["NOME", "IDADE"]
		
		# Act
		actual_members = [member.name for member in Example]
		
		# Assert
		self.assertEqual(expected_members, actual_members)
	
	
	def test_init_method(self):
		"""
		Tests that the init method correctly returns a dictionary of column names to values.
		"""
		# Arrange
		expected_dict = {"NOME": "FERNANDO THEODORO GUIMARÃES", "IDADE": 27}
		
		# Act
		actual_dict = Example.init()
		
		# Assert
		self.assertEqual(expected_dict, actual_dict)
	
	
	@classmethod
	def tearDownClass(cls):
		pass


if __name__ == "__main__":
	unittest.main()
