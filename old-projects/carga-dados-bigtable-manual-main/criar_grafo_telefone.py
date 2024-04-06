from bigtable.client import BigTableClient
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
# project_id = "dev-data-31ce"
# instance_id = "dc-base-cadastral-dev"

# HML
project_id = "data-88d7"
instance_id = "dc-base-cadastral-hml"

table_id = "base_cadastral_reversed_index_produto"
row_key_origin = '46991178073'
row_key_destiny = row_key_origin


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    input_row_dict = ReadRowBigtableV2(table).read_row(row_key_origin)
    print(r'Row Data: {}'.format(input_row_dict))
    if bool(input_row_dict):
        changed_row = add_data(input_row_dict)
        print(r'Changed Row Data: {}'.format(changed_row))
        print('Writing')
        WriteRowBigTableV2(table).write_row(row_key_destiny, changed_row)
    else:
        print('Empty origin')


def add_data(input_data: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    input_data['telefone']['00084070349596'] = '00084070349596'
    return input_data


if __name__ == "__main__":
    run()
