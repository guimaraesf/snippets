import os
from dependency_injector import containers, providers
from dependency_injector.wiring import Provide, inject
from python.utils.logger import Logger
from python.utils.enums import Example

__PATH__ = os.path.join(os.path.dirname(__file__), "../../config/config.json")


class Container(containers.DeclarativeContainer):
    json_config = providers.Configuration(json_files=[__PATH__])
    logger = providers.Object(Logger.init())
    enums = providers.Object(Example.init())


class MyClass:
    def __init__(
        self,
        config=Provide[Container.json_config],
        logger=Provide[Container.logger],
        enums=Provide[Container.enums],
    ) -> None:
        self.cfg = config
        self.logger = logger
        self.enums = enums

    @inject
    def main(self):
        self.logger.info("As dependências foram injetadas...")


if __name__ == "__main__":
    container = Container()



    run = MyClass()
    run.main()
