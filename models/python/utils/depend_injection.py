import os
from dependency_injector import containers, providers
from dependency_injector.wiring import Provide, inject
import logging
from logging import config

__PATH__ = os.path.join(os.path.dirname(__file__), "../../config/config.json")
__LOG_PATH__ = os.path.join(os.path.dirname(__file__), "../../config/logging.ini")


class Logger:
    """
    Logger class for the application.
    """

    _instance = None

    @staticmethod
    def get_instance():
        """

        Returns
        -------

        """
        if Logger._instance is None:
            Logger()
        return Logger._instance

    def __init__(self, config_file: str = __LOG_PATH__) -> None:
        """
        Initializes the logger with the specified log level.

        Parameters
        ----------
        config_file : logging, optional
            The config file of logging to be used.
        """
        if Logger._instance is not None:
            raise Exception("This class is a Singleton")
        else:
            self.config_file = config_file
            self.logger = self._config_and_get_logger()
            Logger._instance = self

    def _config_and_get_logger(self) -> logging.Logger:
        """
        Returns the root logger.

        Returns
        -------
        logging.Logger
            The root logger.
        """
        logging.config.fileConfig(self.config_file)
        return logging.getLogger()


class Config(containers.DeclarativeContainer):
    json_config = providers.Configuration(json_files=[__PATH__])

    object_provider = providers.Object(__PATH__)
    
    logger_instance = Logger.get_instance().logger
    logger = providers.Object(logger_instance)


@inject
class MyClass:
    def __init__(
        self,
        cfg = Provide[Config.json_config],
        logger = Provide[Config.logger]
    ) -> None:
        self.cfg = cfg
        self.logger = logger

    def main(self):
        self.logger.info("Testando container")
        print(self.cfg)

if __name__ == "__main__":
    c = Config()
    c.init_resources()
    c.wire(modules=[__name__])
    run = MyClass()
    run.main()