import datetime

from google.cloud.bigtable.table import Table

from bigtable.utils import UtilsBigTable


class WriteRowBigTable:

    def __init__(self, table: Table):
        super().__init__()
        self._table = table

    def write_row(self, row_key_prefix: str, input_data: dict[str, str]):
        timestamp = datetime.datetime.utcnow()
        row_key = UtilsBigTable.get_row_key(row_key_prefix)
        row = self._table.direct_row(row_key.encode())
        for key, value in input_data.items():
            if key == "chave":
                row.set_cell("reg", "chave", row_key, timestamp)
            elif key == "key":
                row.set_cell("reg", "key", row_key, timestamp)
            elif key == "num_cpf_cnpj":
                row.set_cell("reg", "num_cpf_cnpj", row_key_prefix, timestamp)
            else:
                row.set_cell("reg", key, value.encode(), timestamp)
        row.commit()
        print(r"Row added {}".format(row_key))
