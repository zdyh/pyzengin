import json
import sqlite3
from csv import DictReader
from pathlib import Path

from mojimoji import han_to_zen

FIELDS = [
    'bank_code',
    'branch_code',
    'han_kana',
    'name',
    'branch_flag',  # 名称区分 １：銀行名称 ２：支店名称
    'sub_branch'  # 出張所フラグ １：重複なし or 母店 ２：出張所
]

DB_PATH = Path(__file__).parent / 'zengin.db'


def build_json(bank_csv):
    data = {}
    with open(bank_csv, 'rt', newline='\r\n') as fd:
        for row in DictReader(fd, FIELDS):
            han_kana = row.pop('han_kana').strip()
            row['zen_kana'] = han_to_zen(han_kana)
            row['han_kana'] = han_kana

            if row.pop('branch_flag') == '1':
                row.pop('branch_code')
                row.pop('sub_branch')
                row['branches'] = {}
                data[row['bank_code']] = row
            else:
                branches = data[row.pop('bank_code')]['branches']
                if row['sub_branch'] == '1':
                    branches[row['branch_code']] = row
                else:
                    branch_code = row['branch_code']
                    if 'sub_branches' not in branches[branch_code]:
                        branches[branch_code]['sub_branches'] = []
                    branches[branch_code]['sub_branches'].append(row)

    # write json file
    with open('zengin/zengindata.json', 'w') as outfile:
        outfile.write(json.dumps(data, ensure_ascii=False, indent=2))
    return data


def write_to_sqlite_db(data, db_file=DB_PATH):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS bank")
    cursor.execute("""
        CREATE TABLE bank(
          bank_code TEXT PRIMARY KEY,
          name TEXT, 
          zen_kana TEXT, 
          han_kana TEXT
        )""")
    cursor.execute("DROP TABLE IF EXISTS branch")
    cursor.execute("""
        CREATE TABLE branch(
          bank_code TEXT,
          branch_code TEXT, 
          name TEXT, 
          zen_kana TEXT, 
          han_kana TEXT,
          sub_branch TEXT
        )""")

    cursor.execute("CREATE INDEX ix_code ON branch(bank_code, branch_code)")

    for bank in data.values():
        cursor.execute("INSERT INTO bank(bank_code, name, zen_kana, han_kana)VALUES(?, ?, ?, ?)",
                       (bank['bank_code'], bank['name'], bank['zen_kana'], bank['han_kana']))
        for branch in bank['branches'].values():
            cursor.execute("INSERT INTO BRANCH(bank_code, branch_code, name, zen_kana, han_kana, sub_branch) "
                           "VALUES(?, ?, ?, ?, ?, ?)",
                           (bank['bank_code'], branch['branch_code'], branch['name'], branch['zen_kana'],
                            branch['han_kana'], branch['sub_branch']))
            if 'sub_branches' in branch:
                for sub in branch['sub_branches']:
                    cursor.execute("INSERT INTO BRANCH(bank_code, branch_code, name, zen_kana, han_kana, sub_branch) "
                                   "VALUES(?, ?, ?, ?, ?, ?)",
                                   (bank['bank_code'], sub['branch_code'], sub['name'], sub['zen_kana'],
                                    sub['han_kana'], sub['sub_branch']))
    conn.commit()
    conn.close()


if __name__ == '__main__':
    data = build_json('raw/ginkositen.utf8.csv')
    write_to_sqlite_db(data)
