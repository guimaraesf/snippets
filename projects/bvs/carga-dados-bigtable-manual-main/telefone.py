from bigtable.client import BigTableClient
from bigtable.read_row import ReadRowBigtable
from bigtable.utils import UtilsBigTable
from bigtable.write_row import WriteRowBigTable

# DEV
project_id = "dev-data-31ce"
instance_id = "dc-base-cadastral-dev"
row_key_origin = "0001d8ea8f94d6d276dd8ae4c14c61cb-0"

# HML
# project_id = "data-88d7"
# instance_id = "dc-base-cadastral-hml"
# row_key_origin = "0005460cb46dc97a0ebc07a9270c0130-0"

table_id = "base_cadastral_telefone_produto"
row_key_prefix_destiny = "00054239270480"
cpf = "00054239270480"


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    input_row_dict = ReadRowBigtable(table).read_row(row_key_origin)
    print(r"Row Data: {}".format(input_row_dict))
    if bool(input_row_dict):
        changed_row = change_cpf(cpf, input_row_dict)
        print(r"Changed Row Data: {}".format(changed_row))
        print("Writing")
        WriteRowBigTable(table).write_row(row_key_prefix_destiny, input_row_dict)
    else:
        print("Empty origin")


def change_cpf(cpf: str, input_data: dict[str, str]):
    for key in input_data.keys():
        if key in ("chave", "key"):
            input_data[key] = UtilsBigTable.get_row_key(cpf)
        elif key == "num_cpf_cnpj":
            input_data[key] = cpf
    return input_data


if __name__ == "__main__":
    run()
