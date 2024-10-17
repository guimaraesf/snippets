import logging
import os
import sys
import unittest
from python.utils.logger import Logger


class TestLogger(unittest.TestCase):
	def setUp(self):
		self.temp_config_file = os.path.join(os.getcwd(), "test_logging.ini")
		with open(self.temp_config_file, "w") as f:
			f.write("""
				[loggers]
				keys=root
				
				[handlers]
				keys=consoleHandler
				
				[formatters]
				keys=consoleFormatter
				
				[logger_root]
				level=DEBUG
				handlers=consoleHandler
				
				[handler_consoleHandler]
				class=StreamHandler
				level=DEBUG
				formatter=consoleFormatter
				args=(sys.stdout,)
				
				[formatter_consoleFormatter]
				format=%(asctime)s %(levelname)s %(module)s %(message)s
				datefmt=%Y-%m-%d %H:%M:%S
			""")
	
	
	def test_init(self):
		# Act
		logger = Logger.init(self.temp_config_file)
		
		# Assert
		self.assertIsInstance(logger, logging.Logger)
	
	
	def test_singleton(self):
		# Act
		logger1 = Logger.init()
		logger2 = Logger.init()
		
		# Assert
		self.assertIs(logger1, logger2)
	

	def tearDown(self):
		os.remove(self.temp_config_file)


if __name__ == '__main__':
	unittest.main()
