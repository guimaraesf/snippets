from bigtable.client import BigTableClient
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
# project_id = "dev-data-31ce"
# instance_id = "dc-base-cadastral-dev"

# HML
project_id = "data-88d7"
instance_id = "dc-base-cadastral-hml"

table_id = "base_cadastral_ppe_relacionados_produto"
row_key_prefix = 'd639fc650b9085fded6f6868dc7805e0#40090663187'

data_to_add = [(
    row_key_prefix,
    {
        'titular': {
            'CHAVE': 'd639fc650b9085fded6f6868dc7805e0',
            'NOM_RZO_SCL': '',
            'DSC_TPO_REL': 'IRMA / IRMAO',
            'NUM_CPF_REL': '17322515412',
            'NUM_CPF_TIT': '40090663187',
            'key': row_key_prefix
        }
    }
)]


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    print(r'Row Data: {}'.format(data_to_add))
    if bool(data_to_add):
        print('Writing')
        for data in data_to_add:
            WriteRowBigTableV2(table).write_row(data[0], data[1])
    else:
        print('Empty origin')



if __name__ == "__main__":
    run()
