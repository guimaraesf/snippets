import hashlib


class UtilsBigTable:

    @staticmethod
    def get_row_key(key: str) -> str:
        return hashlib.md5(key.encode('utf-8')).hexdigest() + '-0'
