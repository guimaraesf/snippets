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
    "40090663187",
    "05721913169",
    "12030902187",
    "33915890197",
    "56417543153",
    "60276282191",
    "96098716153",
]

table_id = "base_cadastral_telefone_produto"


def run():
    bigtable_from = BigTableClient(project_id_dev, instance_id_dev).get_instance()
    table_from = bigtable_from.table(table_id)
    for prefix in prefixes:
        cpf_hashed = hashlib.md5(("000" + prefix).encode("utf-8")).hexdigest()
        print(cpf_hashed)
        rows = ReadRowBigtableV2(table_from).read_rows(cpf_hashed, 100)
        print(r"Row Data: {}".format(rows))
        if bool(rows):
            print("Writing")
            bigtable_to = BigTableClient(project_id_hml, instance_id_hml).get_instance()
            table_to = bigtable_to.table(table_id)
            for row in rows:
                # pass
                WriteRowBigTableV2(table_to).write_row(row[0], row[1])
        else:
            print("Empty origin: {}".format(prefix))


if __name__ == "__main__":
    run()
