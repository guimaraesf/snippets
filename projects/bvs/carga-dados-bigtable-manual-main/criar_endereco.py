from bigtable.client import BigTableClient
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
project_id = "dev-data-31ce"
instance_id = "dc-base-cadastral-dev"

# HML
# project_id = "data-88d7"
# instance_id = "dc-base-cadastral-hml"

table_id = "base_cadastral_reversed_index_produto"
row_key_destiny = "NOME FANTASIA DO CPF 00054239270480"


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    input_row_dict = {
        "nome": {"00054239270480": "00054239270480"},
        "nome-nascimento": {"1997-05-07": "00054239270480"},
    }
    print(r"Row Data: {}".format(input_row_dict))
    if bool(input_row_dict):
        print("Writing")
        WriteRowBigTableV2(table).write_row(row_key_destiny, input_row_dict)
    else:
        print("Empty origin")


if __name__ == "__main__":
    run()
