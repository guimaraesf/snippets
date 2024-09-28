from bigtable.client import BigTableClient
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
# project_id = "dev-data-31ce"
# instance_id = "dc-base-cadastral-dev"

# HML
project_id = "data-88d7"
instance_id = "dc-base-cadastral-hml"

table_id = "base_cadastral_nome_produto"
row_key_origin = "000004d6bfed33a23a753be24bdafcb6-0"
row_key_destiny = "8781b01bef27853ea6cb449a78d258ad-0"
new_data = {
    "nom_fnts_socl": "NOME FANTASIA DO CPF 00012030902187",
    "nom_raz_soc": "NOME DO CPF 00012030902187",
    "dat_nsc": "2020-05-07",
    "num_cpf_cnpj": "00012030902187",
}


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    input_row_dict = ReadRowBigtableV2(table).read_row(row_key_origin)
    print(r"Row Data: {}".format(input_row_dict))
    if bool(input_row_dict):
        changed_row = change_data(input_row_dict)
        print(r"Changed Row Data: {}".format(changed_row))
        print("Writing")
        WriteRowBigTableV2(table).write_row(row_key_destiny, input_row_dict)
    else:
        print("Empty origin")


def change_data(input_data: dict[str, dict[str, str]]):
    for family, qualifier in input_data.items():
        for key in qualifier.keys():
            if key in new_data.keys():
                input_data[family][key] = new_data[key]
    return input_data


if __name__ == "__main__":
    run()
