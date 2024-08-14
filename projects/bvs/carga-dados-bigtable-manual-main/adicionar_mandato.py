from bigtable.client import BigTableClient
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
# project_id = "dev-data-31ce"
# instance_id = "dc-base-cadastral-dev"

# HML
project_id = "data-88d7"
instance_id = "dc-base-cadastral-hml"

table_id = "base_cadastral_ppe_mandatos_produto"
row_key = '68cefbb8789fdf4efdc36c6719850d89'

rows = [
    ('68cefbb8789fdf4efdc36c6719850d89-96-52631-2005-01-01', {
        'reg': {
            'COD_PPE_CRG': '96',
            'COD_PPE_ORG': '52631',
            'DAT_FIM_ATV': '2006-01-01',
            'DAT_INI_ATV': '2005-01-01',
            'DSC_PPE_CRG': 'SUBSECRETARIO',
            'DSC_PPE_ESF_PDR': 'TESTE',
            'DSC_PPE_ORG': 'SECRETARIA EXECUTIVA DO MINISTERIO DO DESENVOLVIMENTO SOCIAL E COMBATE A FOME - MDS                                                                   ',
            'NUM_CPF': '40090663187'
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


if __name__ == "__main__":
    run()
