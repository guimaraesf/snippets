from bigtable.client import BigTableClient
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
# project_id = "dev-data-31ce"
# instance_id = "dc-base-cadastral-dev"

# HML
project_id = "data-88d7"
instance_id = "dc-base-cadastral-hml"

table_id = "base_cadastral_ppe_orgaos_produto"
row_key_prefix = '68cefbb8789fdf4efdc36c6719850d89'

rows = [
    ('97466', {
        'reg': {
            'COD_PPE_ESF_PDR': '3',
            'COD_PPE_ORG': '97466',
            'DSC_PPE_ESF_PDR': 'PODER EXECUTIVO FEDERAL',
            'DSC_PPE_ORG': 'SECRETARIA-EXECUTIVA / SUBSECRETARIA DE PLANEJAMENTO, ORCAMENTO E GOVERNANCA                                                                          ',
            'key': '97466'
        }
    })
]


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    print(r'Row Data: {}'.format(rows))
    if bool(rows):
        print('Writing')
        for row in rows:
            WriteRowBigTableV2(table).write_row(row[0], row[1])
    else:
        print('Empty origin')


def add_cod(input_data: [tuple[str, dict[str, dict[str, str]]]]) -> [tuple[str, dict[str, dict[str, str]]]]:
    for idx, row in enumerate(input_data):
        key = row[0].split('-')
        for family, data in (row[1].items()):
            input_data[idx][1][family]['COD_PPE_CRG'] = key[1]
            input_data[idx][1][family]['COD_PPE_ORG'] = key[2]
    return input_data


if __name__ == "__main__":
    run()
