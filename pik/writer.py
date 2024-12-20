from pik.util import format_invoice, is_invoice_zero
from collections import defaultdict

import os
import csv
import logging

def write_invoices_to_files(invoices, conf):
    out_dir = conf["out_dir"]
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    invoice_format_id = conf.get("invoice_format", "2015")
    for invoice in invoices:
        account = invoice.account_id
        with open(os.path.join(out_dir, account + ".txt"), "w", encoding="utf-8") as f:
            f.write(format_invoice(invoice, conf["description"], invoice_format_id))

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

def write_outputs(invoices, conf):
    """Write all output files"""
    out_dir = conf["out_dir"]
    # Handle existing output directory
    if os.path.exists(out_dir):
        logging.getLogger('pik.writer').warning("Output directory already exists: %s", out_dir)
        # Clear existing files in the directory
        for file in os.listdir(out_dir):
            if file.endswith(".txt") or file.endswith(".csv"):
                os.remove(os.path.join(out_dir, file))
    else:
        os.makedirs(out_dir)
    
    valid_invoices = [i for i in invoices if not is_invoice_zero(i)]
    invalid_invoices = [i for i in invoices if is_invoice_zero(i)]

    write_invoices_to_files(valid_invoices, conf)
    write_invoices_to_files(invalid_invoices, conf)
    
    total_csv_fname = conf.get("total_csv_name", os.path.join(out_dir, "totals.csv"))
    row_csv_fname_template = conf.get("row_csv_name_template", os.path.join(out_dir, "rows_%s.csv"))
    
    write_total_csv(invoices, total_csv_fname)
    write_row_csv(invoices, row_csv_fname_template)
    
    return valid_invoices, invalid_invoices
