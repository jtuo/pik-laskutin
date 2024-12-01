import os
import csv
from pik.util import format_invoice
from collections import defaultdict

def write_invoices_to_files(invoices, conf):
    out_dir = conf["out_dir"]
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    invoice_format_id = conf.get("invoice_format", "2015")
    for invoice in invoices:
        account = invoice.account_id
        with open(os.path.join(out_dir, account + ".txt"), "wb") as f:
            f.write(format_invoice(invoice, conf["description"], invoice_format_id).encode("utf-8"))

def write_total_csv(invoices, fname):
    import csv
    with open(fname, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(invoice.to_csvrow_total() for invoice in invoices)

def write_row_csv(invoices, fname_template):
    by_year = defaultdict(lambda: [])
    for invoice in invoices:
        for line in invoice.lines:
            if not line.rollup:
                row = line.to_csvrow()
                by_year[row.ledger_year].append(row)
    for year, yearly_rowset in by_year.items():
        with open(fname_template%year, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(yearly_rowset)