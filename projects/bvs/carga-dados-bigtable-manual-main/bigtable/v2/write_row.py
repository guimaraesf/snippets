import datetime

from google.cloud.bigtable.table import Table


class WriteRowBigTableV2:

    def __init__(self, table: Table):
        super().__init__()
        self._table = table

    def write_row(self, row_key: str, input_data: dict[str, dict[str, str]]):
        timestamp = datetime.datetime.utcnow()
        row = self._table.direct_row(row_key.encode())
        for family, qualifier in input_data.items():
            for column, value in qualifier.items():
                row.set_cell(family, column, value.encode(), timestamp)
        row.commit()
        print(r"Row added {}".format(row_key))

    def write_rows(self, input_data_list: tuple[str, dict[str, dict[str, str]]]):
        timestamp = datetime.datetime.utcnow()
        rows = []
        for data in input_data_list:
            row = self._table.direct_row(data[0].encode())
            for family, qualifier in data[1].items():
                for column, value in qualifier.items():
                    row.set_cell(family, column, value.encode(), timestamp)
            rows.append(row)
            if len(rows) == 5000:
                response = self._table.mutate_rows(rows)
                print(r"Rows added {}".format((len(rows))))
                rows = []
                for i, status in enumerate(response):
                    if status.code != 0:
                        print("Error writing row: {}".format(status.message))

        if bool(rows):
            response = self._table.mutate_rows(rows)
            for i, status in enumerate(response):
                if status.code != 0:
                    print("Error writing row: {}".format(status.message))

        print(r"Rows added {}".format((len(rows))))
