import re
import sqlite3
from dataclasses import dataclass, astuple
from pathlib import Path
from typing import Union, List

DB_PATH = Path(__file__).parent / 'zengin.db'

CONN = sqlite3.connect(DB_PATH.absolute())
DB = CONN.cursor()

kana_table = str.maketrans(
    'ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽ'
    'まみむめもゃやゅゆょよらりるれろわをんーゎゐゑゕゖゔァィゥェォッャュョヮヵヶヰヱヲ',
    'アアイイウウエエオオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポ'
    'マミムメモヤヤユユヨヨラリルレロワオンーワイエカケヴアイウエオツヤユヨワカケイエオ')


def full_katakana(name: str) -> bool:
    p = re.compile(r'[\u30A1-\u30FF]+')
    m = p.fullmatch(name)
    return m is not None and m.endpos == len(name)


class Zengin:
    """A class for managing DB connections.
    In a multi-threaded environment you don't want to share the default DB
    connection. This class encapsulates a DB connection and can be used as a
    context manager.
    """

    def __init__(self):
        self._conn = sqlite3.connect(DB_PATH)
        self._db = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()

    def get(self, code):
        return get(code, self._db)


@dataclass(frozen=True)
class Bank:
    _bank_stmt = "SELECT bank_code, bank_name, bank_full_name, bank_zen_kana, bank_han_kana FROM bank "
    _branch_stmt = "SELECT branch_code, branch_name, branch_zen_kana, branch_han_kana FROM branch WHERE bank_code = ? "
    bank_code: str
    bank_name: str
    bank_full_name: str
    bank_zen_kana: str
    bank_han_kana: str

    @classmethod
    def is_valid_bank_code(cls, code: Union[int, str]) -> bool:
        try:
            code = str(int(code)).zfill(4)
            return len(code) == 4
        except ValueError:
            return False

    @classmethod
    def get(cls, bank_code: Union[int, str]):
        if not cls.is_valid_bank_code(bank_code):
            raise ValueError
        bank_code = str(int(bank_code)).zfill(4)
        DB.execute(cls._bank_stmt + "WHERE bank_code = ?", (bank_code,))
        res = DB.fetchone()
        if res:
            return cls(*res)

    @classmethod
    def search(cls, key: str):
        if cls.is_valid_bank_code(key):
            res = cls.get(key)
            return [res] if res else []

        if full_katakana(key.translate(kana_table)):
            DB.execute(cls._bank_stmt + "WHERE bank_zen_kana LIKE ?", (f'{key.translate(kana_table)}%',))
        else:
            DB.execute(cls._bank_stmt + "WHERE bank_name LIKE ?", (f'{key}%',))
        res = DB.fetchall()
        if len(res):
            return [cls(*r) for r in res]
        else:
            return []

    @classmethod
    def major_banks(cls):
        DB.execute(cls._bank_stmt + "WHERE bank_code in ('0001', '0005', '0009', '0010', '0017')")
        res = DB.fetchall()
        if len(res):
            return [cls(*r) for r in res]
        else:
            return []

    @property
    def branches(self):
        DB.execute(self._branch_stmt + "ORDER BY branch_code", (self.bank_code,))
        res = DB.fetchall()
        if len(res):
            return [Branch(*(astuple(self) + r)) for r in res]

    def get_branch(self, branch_code: Union[int, str]):
        if not Branch.is_valid_branch_code(branch_code):
            raise ValueError('%r is not valid branch code' % branch_code)

        branch_code = str(int(branch_code)).zfill(3)
        DB.execute(self._branch_stmt + "AND branch_code=?", (self.bank_code, branch_code,))
        res = DB.fetchall()
        if len(res):
            return [Branch(*(astuple(self) + r)) for r in res]
        else:
            return []

    def search_branch(self, key: str):
        if full_katakana(key.translate(kana_table)):
            DB.execute(self._branch_stmt + ' AND branch_zen_kana like ?',
                       (self.bank_code, f'{key.translate(kana_table)}%'))
        else:
            DB.execute(self._branch_stmt + ' AND branch_name like ?', (self.bank_code, f'{key}%'))

        res = DB.fetchall()
        if len(res):
            return [Branch(*(astuple(self) + r)) for r in res]
        else:
            return []


@dataclass(frozen=True)
class Branch(Bank):
    branch_code: str
    branch_name: str
    branch_zen_kana: str
    branch_han_kana: str

    @classmethod
    def is_valid_branch_code(cls, code: Union[int, str]) -> bool:
        try:
            code = str(int(code)).zfill(3)
            return len(code) == 3
        except ValueError:
            return False

    @property
    def branch_full_name(self):
        if self.branch_name is None:
            return None
        if self.bank_code == '9900':
            return self.branch_name
        if '営業' in self.branch_name:
            return self.branch_name
        if self.branch_name.endswith('店'):
            return self.branch_name
        if self.branch_name.endswith('出張所'):
            return self.branch_name
        return self.branch_name + '支店'

    def __str__(self):
        return self.__repr__()[:-1] + f", branch_full_name='{self.branch_full_name}')"

    @classmethod
    def get(cls, bank_code: Union[int, str], branch_code: Union[int, str]) -> List["Branch"]:
        if not cls.is_valid_bank_code(bank_code):
            raise ValueError('%r is not valid bank code' % bank_code)

        bank = Bank.get(bank_code)
        if bank:
            return bank.get_branch(branch_code)

    @classmethod
    def search(cls, bank_code, key: str):
        if not cls.is_valid_bank_code(bank_code):
            raise ValueError('%r is not valid bank code' % bank_code)

        bank = Bank.get(bank_code)
        if bank:
            return bank.search_branch(key)
        else:
            return []


def get(bank_code: str, branch_code):
    return Branch.get(bank_code, branch_code)
