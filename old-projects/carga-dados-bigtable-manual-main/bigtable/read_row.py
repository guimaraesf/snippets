from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.table import Table
from google.cloud.bigtable_v2 import Row


class ReadRowBigtable:

    def __init__(self, table: Table):
        super().__init__()
        self._table = table
        self._col_filter = row_filters.CellsColumnLimitFilter(1)

    def read_row(self, row_key: str) -> dict[str, str]:
        row = self._table.read_row(row_key.encode(), self._col_filter)
        return self._row_to_dict(row)

    def read_rows(self, prefix: str, limit: int = 1) -> [dict[str, str]]:
        end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        row_set = RowSet()
        row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
        rows = self._table.read_rows(row_set=row_set, filter_=self._col_filter, limit=limit)
        result = []
        if rows is not None:
            for row in rows:
                result.append(self._row_to_dict(row))
        return result

    def _row_to_dict(self, row: Row) -> dict[str, str]:
        row_dct = {}
        if row is not None:
            for cf, cols in sorted(row.cells.items()):
                for col, cells in sorted(cols.items()):
                    for cell in cells:
                        key = col.decode('utf-8')
                        value = cell.value.decode('utf-8')
                        row_dct[key] = value

        return row_dct
