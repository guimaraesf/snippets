from google.cloud import bigtable
from google.cloud.bigtable.instance import Instance


class BigTableClient:

    def __init__(self, project_id, instance_id):
        super().__init__()
        self._client = None
        self._instance = None
        self.project_id = project_id
        self.instance_id = instance_id

    def get_instance(self):
        if self._instance is None:
            self._create_instance()
        return self._instance

    def _get_client(self) -> bigtable:
        if self._client is None:
            self._client = bigtable.Client(project=self.project_id, admin=True)
        return self._client

    def _create_instance(self) -> Instance:
        if self._instance is None:
            self._instance = self._get_client().instance(self.instance_id)
        return self._instance
