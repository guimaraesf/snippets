from bigtable.client import BigTableClient
from bigtable.utils import UtilsBigTable
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
# project_id = "dev-data-31ce"
# instance_id = "dc-base-cadastral-dev"
row_key_origin = "39a46da628d7a43a98fd92e6b8d49e35-0"

# HML
project_id = "data-88d7"
instance_id = "dc-base-cadastral-hml"
# row_key_origin = "00000d24ca340d1d1e36a1eaf24f6fce-2"

table_id = "base_cadastral_endereco_new_produto"
num_cpf_cnpj_destiny = "00040090663187"
row_key_destiny = UtilsBigTable.get_row_key(num_cpf_cnpj_destiny)


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    input_row_dict = ReadRowBigtableV2(table).read_row(row_key_origin)
    print(r"Row Data: {}".format(input_row_dict))
    if bool(input_row_dict):
        changed_row = change_data(input_row_dict)
        print(r"Changed Row Data: {}".format(changed_row))
        print("Writing")
        WriteRowBigTableV2(table).write_row(num_cpf_cnpj_destiny, changed_row)
    else:
        print("Empty origin")


def change_data(input_data: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    for family, qualifier in input_data.items():
        for key in qualifier.keys():
            if key in ("num_cpf_cnpj"):
                input_data[family][key] = num_cpf_cnpj_destiny
            elif key in ("key", "chave"):
                input_data[family][key] = row_key_destiny
            elif key in ("index", "indice"):
                input_data[family][key] = "0"
            elif key == "key_household":
                input_data[family][key] = "df80a095446f3e3c8e06bc006f34b45d"
    return input_data


if __name__ == "__main__":
    run()
