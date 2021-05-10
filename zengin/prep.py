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

    # same name list
    # 0401 シティバンク、エヌ・エイ
    # 0403 バンク・オブ・アメリカ・エヌ・エイ
    # 0429 バンクネガラインドネシア
    # 0482 アイエヌジー　バンク　エヌ・ヴィ
    # 0484 ナショナル・オーストラリア・バンク・リミテッド
    # 0617 フィリピン・ナショナル・バンク
    # 1000 信金中央金庫
    # 2213 整理回収機構
    for code in ['0401', '0403', '0429', '0482', '0484', '0617', '1000', '2213']:
        bank = data[code]
        bank['full_name'] = bank['name']

    # specific cases
    for code, _, expand in zip(['2004', '2010', '3000', '3771'], ['商工中金', '全信組連', '農林中金', '秋田たかのす'],
                               ['商工組合中央金庫', '全国信用協同組合連合会', '農林中央金庫', '秋田たかのす農業協同組合']):
        bank = data[code]
        bank['full_name'] = expand

    # abbreviation of financial institutions
    for bank in data.values():
        for abbr, expand in zip(
                ['農協', '漁協', '信漁連', '信組', '信連', '労金', '信金'],
                ['農業協同組合', '漁業協同組合', '信用漁業協同組合連合会', '信用組合', '信用連合会', '労働金庫', '信用金庫']):
            if 'full_name' not in bank:
                if bank['name'].endswith(abbr):
                    bank['full_name'] = bank['name'][:-len(abbr)] + expand

    # banks
    for bank in data.values():
        if 'full_name' not in bank:
            bank['full_name'] = bank['name'] + '銀行'

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
          bank_name TEXT, 
          bank_full_name TEXT, 
          bank_zen_kana TEXT, 
          bank_han_kana TEXT
        )""")
    cursor.execute("DROP TABLE IF EXISTS branch")
    cursor.execute("""
        CREATE TABLE branch(
          bank_code TEXT,
          branch_code TEXT, 
          branch_name TEXT, 
          branch_zen_kana TEXT, 
          branch_han_kana TEXT,
          sub_branch TEXT
        )""")

    cursor.execute("CREATE INDEX ix_code ON branch(bank_code, branch_code)")

    bank_insert_stmt = """
      INSERT INTO bank(bank_code, bank_name, bank_full_name, bank_zen_kana, bank_han_kana) 
      VALUES(?, ?, ?, ?, ?)"""

    branch_insert_stmt = """
        INSERT INTO BRANCH(bank_code, branch_code, branch_name, branch_zen_kana, branch_han_kana, sub_branch) 
        VALUES(?, ?, ?, ?, ?, ?)"""

    for bank in data.values():
        cursor.execute(bank_insert_stmt,
                       (bank['bank_code'], bank['name'], bank['full_name'], bank['zen_kana'], bank['han_kana']))
        for branch in bank['branches'].values():
            cursor.execute(branch_insert_stmt,
                           (bank['bank_code'], branch['branch_code'], branch['name'], branch['zen_kana'],
                            branch['han_kana'], branch['sub_branch']))
            if 'sub_branches' in branch:
                for sub in branch['sub_branches']:
                    cursor.execute(branch_insert_stmt,
                                   (bank['bank_code'], sub['branch_code'], sub['name'], sub['zen_kana'],
                                    sub['han_kana'], sub['sub_branch']))
    conn.commit()
    conn.close()


if __name__ == '__main__':
    data = build_json('raw/ginkositen.utf8.csv')
    write_to_sqlite_db(data)
