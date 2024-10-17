import logging
import unittest

from python.utils.containers import Container
from dependency_injector import providers
from unittest import mock
from unittest.mock import patch, Mock
from python.utils.enums import Example


class TestContainer(unittest.TestCase):
	def test_container(self):
		# Arrange
		container = Container()
		json_config = {
			"aws": {
				"access_key_id": "KEY",
				"secret_access_key": "SECRET"
			}
		}

		# Act
		json = container.json_config()
		enums = container.enums()
		logger = container.logger()

		# Assert
		self.assertIsInstance(container.json_config, providers.Configuration)
		self.assertIsInstance(container.logger, providers.Object)
		self.assertIsInstance(container.enums, providers.Object)

		self.assertIsNotNone(container.logger)
		self.assertIsNotNone(container.enums)

		self.assertEqual(json, json_config)
		self.assertEqual(enums["NOME"], "FERNANDO THEODORO GUIMARÃES")
		self.assertIsInstance(logger, logging.RootLogger)


if __name__ == '__main__':
	unittest.main()
