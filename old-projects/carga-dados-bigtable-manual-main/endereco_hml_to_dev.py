from bigtable.client import BigTableClient
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
project_id_dev = "dev-data-31ce"
instance_id_dev = "dc-base-cadastral-dev"

# HML
project_id_hml = "data-88d7"
instance_id_hml = "dc-base-cadastral-hml"
row_key_origin = "39a46da628d7a43a98fd92e6b8d49e35-0"

table_id = "base_cadastral_endereco_new_produto"


def run():
    bigtable_instance_hml = BigTableClient(project_id_hml, instance_id_hml).get_instance()
    table_hml = bigtable_instance_hml.table(table_id)
    input_row_dict = ReadRowBigtableV2(table_hml).read_row(row_key_origin)
    print(r'Row Data: {}'.format(input_row_dict))
    if bool(input_row_dict):
        print('Writing')
        bigtable_instance_dev = BigTableClient(project_id_dev, instance_id_dev).get_instance()
        table_dev = bigtable_instance_dev.table(table_id)
        WriteRowBigTableV2(table_dev).write_row(row_key_origin, input_row_dict)
    else:
        print('Empty origin')

if __name__ == "__main__":
    run()
