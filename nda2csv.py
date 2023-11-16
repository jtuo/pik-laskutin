# -*- coding: utf-8 -*-
from pik import nda
from collections import defaultdict
import csv

def by_account(txn_stream):
    result = defaultdict(lambda: [])
    for txn in txn_stream:
        result[txn.iban].append(txn)
    return result

def to_csv(fobj, account_iban, txns):
    wr = csv.writer(fobj)
    wr.writerow(["", "Tilinumero", account_iban])
    wr.writerow(["Vastatili","Vienti","Kirjauspäivä","Arvopäivä","Maksupäivä","Määrä","Saaja/Maksaja","Tilinumero","BIC","Tapahtuma","Viite","Maksajan viite","Viesti","Kortinnumero","Kuitti"])
    for txn in txns:
        # Row is like:
        # Vastatili,Vienti,Kirjauspäivä,Arvopäivä,Maksupäivä,Määrä,Saaja/Maksaja,Tilinumero,BIC,Tapahtuma,Viite,Maksajan viite,Viesti,Kortinnumero,Kuitti
        wr.writerow(["", "", txn.ledger_date, txn.value_date, txn.payment_date, txn.cents/100.0, txn.name, txn.recipient_iban, txn.recipient_bic, txn.operation, txn.ref, "", txn.msg, txn.receipt])

def main():
    import sys
    if len(sys.argv) < 2:
        print("Usage: nda2csv.py <fname_pattern>", file=sys.stderr)
        print("", file=sys.stderr)
        print("Outputs CSV files in augmented Nordea CSV format based in input Nordea NDA transactions read from stdin", file=sys.stderr)
        print("Each CSV is written into its own file", file=sys.stderr)
        print("", file=sys.stderr)
        print("Arguments:", file=sys.stderr)
        print("  fname_pattern: Output filename pattern, where %s will be replaced by account IBAN number", file=sys.stderr)
        print("", file=sys.stderr)
        print("Example: nda2csv.py < 2016.nda '2016_%s.csv'", file=sys.stderr)
        print("", file=sys.stderr)
        sys.exit(1)
    fname_pattern = sys.argv[1]
    
    # map from account id to list of transactions ordered by time
    account_txns = by_account(nda.transactions(sys.stdin))
    for account, txns in account_txns.items():
        fname = fname_pattern % account
        with open(fname, 'w') as f:
            to_csv(f, account, txns)


if __name__ == '__main__':
    main()
    
    
