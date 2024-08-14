import hashlib

from bigtable.client import BigTableClient
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
project_id_dev = "dev-data-31ce"
instance_id_dev = "dc-base-cadastral-dev"

# HML
project_id_hml = "data-88d7"
instance_id_hml = "dc-base-cadastral-hml"

table_id = "base_cadastral_ppe_endereco_orgao_produto"


def run():
    bigtable_instance_hml = BigTableClient(project_id_hml, instance_id_hml).get_instance()
    table_hml = bigtable_instance_hml.table(table_id)
    rows = ReadRowBigtableV2(table_hml).read_all_rows()
    print(r'Row Data: {}'.format(rows))
    if bool(rows):
        print(len(rows))
        print('Writing')
        bigtable_instance_dev = BigTableClient(project_id_dev, instance_id_dev).get_instance()
        table_dev = bigtable_instance_dev.table(table_id)
        WriteRowBigTableV2(table_dev).write_rows(rows)
    else:
        print('Empty origin')


if __name__ == "__main__":
    run()
