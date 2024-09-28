from bigtable.client import BigTableClient
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
project_id = "dev-data-31ce"
instance_id = "dc-base-cadastral-dev"

# HML
# project_id = "data-88d7"
# instance_id = "dc-base-cadastral-hml"

table_id = "base_cadastral_reversed_index_produto"
row_key_origin = "_13@HOTMAIL.COM"
row_key_destiny = row_key_origin


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    input_row_dict = {"email": {}}
    print(r"Row Data: {}".format(input_row_dict))
    if bool(input_row_dict):
        changed_row = add_data(input_row_dict)
        print(r"Changed Row Data: {}".format(changed_row))
        print("Writing")
        WriteRowBigTableV2(table).write_row(row_key_destiny, changed_row)
    else:
        print("Empty origin")


def add_data(input_data: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    input_data["email"]["00084070349596"] = "00084070349596"
    input_data["email"]["00000027601575"] = "00000027601575"
    input_data["email"]["00000036320351"] = "00000036320351"
    input_data["email"]["00000042312622"] = "00000042312622"
    input_data["email"]["00000058400022"] = "00000058400022"
    input_data["email"]["00000067225778"] = "00000067225778"
    input_data["email"]["00000073279014"] = "00000073279014"
    input_data["email"]["00000094428737"] = "00000094428737"
    return input_data


if __name__ == "__main__":
    run()
