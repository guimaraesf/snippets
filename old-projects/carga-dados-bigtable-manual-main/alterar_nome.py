from bigtable.client import BigTableClient
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
project_id = "dev-data-31ce"
instance_id = "dc-base-cadastral-dev"

# HML
# project_id = "data-88d7"
# instance_id = "dc-base-cadastral-hml"

table_id = "base_cadastral_nome_produto"
row_key_origin = "39a46da628d7a43a98fd92e6b8d49e35-0"
row_key_destiny = row_key_origin
new_name = 'NOME FANTASIA DO CPF 00084070349596'


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    input_row_dict = ReadRowBigtableV2(table).read_row(row_key_origin)
    print(r'Row Data: {}'.format(input_row_dict))
    if bool(input_row_dict):
        changed_row = change_name(new_name, input_row_dict)
        print(r'Changed Row Data: {}'.format(changed_row))
        print('Writing')
        WriteRowBigTableV2(table).write_row(row_key_destiny, input_row_dict)
    else:
        print('Empty origin')


def change_name(name: str, input_data: dict[str, dict[str, str]]):
    for family, qualifier in input_data.items():
        for key in qualifier.keys():
            if key in ('nom_fnts_socl', 'nom_raz_soc'):
                input_data[family][key] = name
    return input_data


if __name__ == "__main__":
    run()
