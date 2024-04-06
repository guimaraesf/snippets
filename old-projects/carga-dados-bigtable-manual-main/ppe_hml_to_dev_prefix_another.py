import hashlib

from bigtable.client import BigTableClient
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
project_id_dev = "dev-data-31ce"
instance_id_dev = "dc-base-cadastral-dev"

# HML
project_id_hml = "data-88d7"
instance_id_hml = "dc-base-cadastral-hml"
prefixes = [
    '40090663187'
]

new_prefix = '17322515412'

table_id = "base_cadastral_ppe_mandatos_produto"


def run():
    bigtable_instance_from = BigTableClient(project_id_hml, instance_id_hml).get_instance()
    table_from = bigtable_instance_from.table(table_id)
    for prefix in prefixes:
        # cpf_hashed = hashlib.md5(('000' + prefix).encode('utf-8')).hexdigest()
        cpf_hashed = hashlib.md5(prefix.encode('utf-8')).hexdigest()
        print(cpf_hashed)
        rows = ReadRowBigtableV2(table_from).read_rows(cpf_hashed, 100)
        print(r'Row Data: {}'.format(rows))
        if bool(rows):
            rows = change_row(hashlib.md5(new_prefix.encode('utf-8')).hexdigest(), rows)
            print(r'Changed Row Data: {}'.format(rows))
            print('Writing')
            bigtable_instance_to = BigTableClient(project_id_hml, instance_id_hml).get_instance()
            table_to = bigtable_instance_to.table(table_id)
            for row in rows:
                # pass
                WriteRowBigTableV2(table_to).write_row(row[0], row[1])
        else:
            print('Empty origin: {}'.format(prefix))

def change_row(new_prefix:str, input_data: [tuple[str, dict[str, dict[str, str]]]]) -> [tuple[str, dict[str, dict[str, str]]]]:
    new_data = []
    for idx, row in enumerate(input_data):
        key = row[0].split('-')
        new_data.append((row[0].replace(key[0], new_prefix), row[1]))
    return new_data


if __name__ == "__main__":
    run()
