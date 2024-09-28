import os
from dependency_injector import containers, providers
from dependency_injector.wiring import Provide, inject
from logger import Logger
from variables import Variables

__PATH__ = os.path.join(os.path.dirname(__file__), "../../config/config.json")

class Container(containers.DeclarativeContainer):
    json_config = providers.Configuration(json_files=[__PATH__])
    logger = providers.Object(Logger.init())
    variables = providers.Object(Variables.init())


class MyClass:
    def __init__(
        self,
        config=Provide[Container.json_config],
        logger=Provide[Container.logger],
        variables=Provide[Container.variables],
    ) -> None:
        self.cfg = config
        self.logger = logger
        self.variables = variables

    @inject
    def main(self):
        self.logger.info("As dependências foram injetadas...")

if __name__ == "__main__":
    container = Container()
    container.init_resources()
    container.wire(modules=[__name__])
    container.check_dependencies()

    run = MyClass()
    run.main()
    