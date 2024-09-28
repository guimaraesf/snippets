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
    # '68cefbb8789fdf4efdc36c6719850d89',
    # '478a195cfadde4472149ed1d4d75d854',
    # '3265b86cd3f63d87e7c6673fb9e08558',
    # '444c72a44d0fc4bcebc31edc1b3bf876',
    # '0c4fc79c6c2af5306caefad3652f33df',
    # 'a8fab36889740cca8652489958172da0',
    # 'e29bcfbde182b0ebc5f180bde0e65924'
    "40090663187",
    "05721913169",
    "12030902187",
    "33915890197",
    "56417543153",
    "60276282191",
    "96098716153",
]

table_id = "base_cadastral_ppe_mandatos_produto"


def run():
    bigtable_instance_hml = BigTableClient(
        project_id_hml, instance_id_hml
    ).get_instance()
    table_hml = bigtable_instance_hml.table(table_id)
    for prefix in prefixes:
        # cpf_hashed = hashlib.md5(('000' + prefix).encode('utf-8')).hexdigest()
        cpf_hashed = hashlib.md5(prefix.encode("utf-8")).hexdigest()
        print(cpf_hashed)
        rows = ReadRowBigtableV2(table_hml).read_rows(cpf_hashed, 100)
        print(r"Row Data: {}".format(rows))
        if bool(rows):
            print("Writing")
            bigtable_instance_dev = BigTableClient(
                project_id_dev, instance_id_dev
            ).get_instance()
            table_dev = bigtable_instance_dev.table(table_id)
            for row in rows:
                # pass
                WriteRowBigTableV2(table_dev).write_row(row[0], row[1])
        else:
            print("Empty origin: {}".format(prefix))


if __name__ == "__main__":
    run()
