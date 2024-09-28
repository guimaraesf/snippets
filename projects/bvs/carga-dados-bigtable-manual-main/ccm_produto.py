from bigtable.client import BigTableClient
from bigtable.read_row import ReadRowBigtable
from bigtable.write_row import WriteRowBigTable

# DEV
# project_id = "dev-data-31ce"
# instance_id = "dc-base-cadastral-dev"
# row_key_origin = "0018fa60e1bba6e2514d49bdfc0991c2-0"
row_key_origin = "001909a259d3c2a72517041b4b22ba45-0"

# HML
project_id = "data-88d7"
instance_id = "dc-base-cadastral-hml"
# row_key_origin = "0005460cb46dc97a0ebc07a9270c0130-0"

table_id = "base_cadastral_ccm_produto"
row_key_prefix_destiny = "19389641000108"
cnpj = "19389641000108"


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    input_row_dict = ReadRowBigtable(table).read_row(row_key_origin)
    print(r"Row Data: {}".format(input_row_dict))
    if bool(input_row_dict):
        changed_row = change_ativo(input_row_dict)
        print(r"Changed Row Data: {}".format(changed_row))
        print("Writing")
        WriteRowBigTable(table).write_row(row_key_prefix_destiny, input_row_dict)
    else:
        print("Empty origin")


def change_ativo(input_data: dict[str, str]) -> dict[str, str]:
    for key in input_data.keys():
        if key == "ativo":
            input_data[key] = "false"
    return input_data


if __name__ == "__main__":
    run()
