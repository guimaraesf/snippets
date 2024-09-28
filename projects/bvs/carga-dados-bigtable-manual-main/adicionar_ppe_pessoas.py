from bigtable.client import BigTableClient
from bigtable.v2.read_row import ReadRowBigtableV2
from bigtable.v2.write_row import WriteRowBigTableV2

# DEV
project_id = "dev-data-31ce"
instance_id = "dc-base-cadastral-dev"

# HML
# project_id = "data-88d7"
# instance_id = "dc-base-cadastral-hml"

table_id = "base_cadastral_ppe_pessoa_produto"
row_key = "68cefbb8789fdf4efdc36c6719850d89"

rows = [
    (
        "77fc722ece75d66003d73c6a47521f80-0",
        {
            "reg": {
                "COD_TPO_PPE": "1",
                "DAT_NSC": "1967-04-28",
                "NOM_PPE": "NOME DO PPE DE CPF 00040090663187",
                # 'NUM_CPF': '17322515412'
                "NUM_CPF_CNPJ": "00040090663187",
            }
        },
    )
]


def run():
    bigtable_instance = BigTableClient(project_id, instance_id).get_instance()
    table = bigtable_instance.table(table_id)
    print(r"Row Data: {}".format(rows))
    if bool(rows):
        print("Writing")
        for row in rows:
            WriteRowBigTableV2(table).write_row(row[0], row[1])
    else:
        print("Empty origin")


if __name__ == "__main__":
    run()
