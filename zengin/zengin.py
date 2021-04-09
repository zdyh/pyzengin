import re
import sqlite3
from collections import namedtuple
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


class Branch(namedtuple('Branch', ['bank_code', 'branch_code', 'branch_name', 'branch_zen_kana', 'branch_han_kana',
                                   'bank_name', 'bank_full_name', 'bank_zen_kana', 'bank_han_kana'],
                        defaults=['', '', '', '', '', '', '', '', ''])):
    @property
    def full_name(self):
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
        return self.__repr__()[:-1] + f", full_name='{self.full_name}')"

    @classmethod
    def search(cls, bank_code, name: str):
        where_stmt = 's.name like ?'
        if full_katakana(name.translate(kana_table)):
            name = name.translate(kana_table)
            where_stmt = 's.zen_kana like ?'

        DB.execute("SELECT s.bank_code, s.branch_code, s.name branch_name, s.zen_kana branch_zen_kana, "
                   "  s.han_kana branch_han_kana, b.name bank_name, b.full_name bank_full_name, "
                   "  b.zen_kana bank_zen_kana, b.han_kana bank_han_kana "
                   "FROM bank b INNER JOIN branch s on b.bank_code = s.bank_code "
                   "WHERE s.bank_code=? and " + where_stmt, (bank_code, name + '%',))
        res = DB.fetchall()
        if len(res):
            return [cls(*r) for r in res]
        else:
            return []

    @classmethod
    def get(cls, bank_code: str, branch_code: Union[int, str]) -> List["Branch"]:
        branch_code = str(int(branch_code)).zfill(3)
        if len(branch_code) != 3:
            raise ValueError

        DB.execute("SELECT s.bank_code, s.branch_code, s.name branch_name, s.zen_kana branch_zen_kana, "
                   "  s.han_kana branch_han_kana, b.name bank_name, b.full_name bank_full_name, "
                   "  b.zen_kana bank_zen_kana, b.han_kana bank_han_kana "
                   "FROM bank b INNER JOIN branch s on b.bank_code = s.bank_code "
                   "WHERE s.bank_code=? and s.branch_code=?", (bank_code, branch_code,))
        res = DB.fetchall()
        if len(res):
            return [cls(*r) for r in res]
        else:
            return []


class Bank(namedtuple('Bank', ['bank_code', 'bank_name', 'bank_full_name', 'bank_zen_kana', 'bank_han_kana'])):
    @classmethod
    def get(cls, bank_code: Union[int, str]):
        code = str(int(bank_code)).zfill(4)
        if len(code) != 4:
            raise ValueError

        DB.execute("select bank_code, name, full_name, zen_kana, han_kana from bank where bank_code = ?", (code,))
        res = DB.fetchone()
        if res:
            return cls(*res)

    @classmethod
    def search(cls, name: str):
        if full_katakana(name.translate(kana_table)):
            DB.execute("SELECT bank_code, name, full_name, zen_kana, han_kana FROM bank WHERE zen_kana LIKE ?",
                       ('%' + name.translate(kana_table) + '%',))
        else:
            DB.execute("SELECT bank_code, name, full_name, zen_kana, han_kana FROM bank WHERE name LIKE ?",
                       ('%' + name + '%',))
        res = DB.fetchall()
        if len(res):
            return [cls(*r) for r in res]
        else:
            return []

    @classmethod
    def major_banks(cls):
        DB.execute("select bank_code, name, full_name, zen_kana, han_kana from bank "
                   "where bank_code in ('0001', '0005', '0009', '0010', '0017')")
        res = DB.fetchall()
        if len(res):
            return [cls(*r) for r in res]
        else:
            return []

    @property
    def branches(self):
        DB.execute("select bank_code, branch_code, name, zen_kana, han_kana from branch "
                   "where bank_code = ? order by branch_code",
                   (self.bank_code,))
        res = DB.fetchall()
        if len(res):
            return [Branch(*r)._replace(**self._asdict()) for r in res]


def get(bank_code: str, branch_code):
    return Branch.get(bank_code, branch_code)
