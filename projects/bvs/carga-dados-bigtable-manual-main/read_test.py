from bigtable.client import BigTableClient
from bigtable.read_row import ReadRowBigtable

# DEV
project_id = "dev-data-31ce"
instance_id = "dc-base-cadastral-dev"
# row_key_origin = "00000d5b8d11c0605f78a54dd05f7b31-1"

# HML
# project_id = "data-88d7"
# instance_id = "dc-base-cadastral-hml"
row_key_prefix = "5bc03934f7d83c3e3dd9372e38d02e27"

table_id = "base_cadastral_scpc_fone_produto"


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    input_row_dict = ReadRowBigtable(table).read_rows(row_key_prefix)
    print(r"Row Data: {}".format(input_row_dict))


if __name__ == "__main__":
    run()
