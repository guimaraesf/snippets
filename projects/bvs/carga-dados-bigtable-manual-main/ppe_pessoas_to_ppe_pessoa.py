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

table_origin_id = "base_cadastral_ppe_pessoas_produto"
table_destiny_id = "base_cadastral_ppe_pessoa_produto"

prefixes = [
    "68cefbb8789fdf4efdc36c6719850d89-0",
    "478a195cfadde4472149ed1d4d75d854-0",
    "3265b86cd3f63d87e7c6673fb9e08558-0",
    "444c72a44d0fc4bcebc31edc1b3bf876-0",
    "0c4fc79c6c2af5306caefad3652f33df-0",
    "a8fab36889740cca8652489958172da0-0",
    "e29bcfbde182b0ebc5f180bde0e65924-0",
    # '40090663187',
    # '05721913169',
    # '12030902187',
    # '33915890197',
    # '56417543153',
    # '60276282191',
    # '96098716153'
]


def run():
    bigtable_instance_origin = BigTableClient(
        project_id_hml, instance_id_hml
    ).get_instance()
    table_origin = bigtable_instance_origin.table(table_origin_id)
    bigtable_instance_dev = BigTableClient(
        project_id_dev, instance_id_dev
    ).get_instance()
    bigtable_instance_hml = BigTableClient(
        project_id_hml, instance_id_hml
    ).get_instance()
    table_destiny_dev = bigtable_instance_dev.table(table_destiny_id)
    table_destiny_hml = bigtable_instance_hml.table(table_destiny_id)
    for prefix in prefixes:
        rows = ReadRowBigtableV2(table_origin).read_rows(prefix)
        print(r"Row Data: {}".format(rows))
        if bool(rows):
            for row in rows:
                changed_row = change_row(row)
                print("Changed row: {}".format(changed_row))
                print("Writing dev")
                WriteRowBigTableV2(table_destiny_dev).write_row(
                    changed_row[0], changed_row[1]
                )
                print("Writing hml")
                WriteRowBigTableV2(table_destiny_hml).write_row(
                    changed_row[0], changed_row[1]
                )

    else:
        print("Empty origin")


def change_row(
    input_data: tuple[str, dict[str, dict[str, str]]]
) -> tuple[str, dict[str, dict[str, str]]]:
    cpf = input_data[1]["reg"]["NUM_CPF"]
    key = hashlib.md5(("000" + cpf).encode("utf-8")).hexdigest() + "-0"
    data_row = input_data[1]
    data_row["reg"]["NUM_CPF_CNPJ"] = "000" + cpf
    data_row["reg"].pop("NUM_CPF")
    return key, data_row


if __name__ == "__main__":
    run()
