import zengin

if __name__ == '__main__':
    print(zengin.Bank.get('0001'))

    for bank in zengin.Bank.search('み'):
        print(bank)

    for branch in zengin.Branch.get('0001', '001'):
        print(branch)

    for branch in zengin.Branch.search('0005', 'キチジョウジ'):
        print(branch)

    for bank in zengin.Bank.major_banks():
        print(bank)

    for branch in zengin.Bank.get('0001').branches:
        print(branch)

    for a in zengin.Bank.search('ユウチヨ'):
        print(a)
