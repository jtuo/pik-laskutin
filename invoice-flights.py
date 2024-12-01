# -*- coding: utf-8
from pik.flights import Flight
from pik.rules import FlightRule, AircraftFilter, PeriodFilter, CappedRule, AllRules, FirstRule, SetDateRule, SimpleRule, SinceDateFilter, ItemFilter, PurposeFilter, InvoicingChargeFilter, TransferTowFilter, NegationFilter, DebugRule, flightFilter, eventFilter, SetLedgerYearRule, PositivePriceFilter, NegativePriceFilter, BirthDateFilter, MinimumDurationRule, MemberListFilter
from pik.util import Period, format_invoice, parse_iso8601_date
from pik.billing import BillingContext, Invoice
from pik.event import SimpleEvent
from pik.hansa import SimpleHansaTransaction, SimpleHansaRow
from pik import nda
import datetime as dt
import csv
import sys
from collections import defaultdict
from itertools import chain, groupby
import json
import os
from itertools import count
import unicodedata
import math
import decimal

def make_rules(ctx=BillingContext(), metadata=None):

    # Configuration
    YEAR = 2024
    F_MOTOR_PERIOD = [PeriodFilter(Period(dt.date(YEAR, 1, 28), dt.date(YEAR, 10, 27)))]
    F_GLIDER_SEASON = F_FULL_YEAR = [PeriodFilter(Period.full_year(YEAR))]

    ACCT_PURSI_KEIKKA = 3220
    ACCT_TOW = 3130
    ACCT_1037 = 3150 # Lentotuntitulot jäseniltä
    ACCT_1037_OPEALE = 3150 # Lentotuntitulot jäseniltä
    ACCT_TOWING = 3170 # Muut lentotoiminnan tulot
    ACCT_PURSI_INSTRUCTION = 3470 # Muut tulot koulutustoiminnasta
    ACCT_KALUSTO = 3010
    ACCT_LASKUTUSLISA = 3610 # Hallinnon tulot

    ID_PURSI_CAP_2024 = f"pursi_hintakatto_{YEAR}"
    ID_KALUSTOMAKSU_CAP_2024 = f"kalustomaksu_hintakatto_{YEAR}"

    birth_dates = (metadata or {}).get("birth_dates", {})
    member_ids = (metadata or {}).get("course_members", set())
    
    F_YOUTH = [BirthDateFilter(birth_dates, 25)]
    F_KURSSI = [MemberListFilter(member_ids)]

    F_FK = [AircraftFilter("650")]
    F_FM = [AircraftFilter("787")]
    F_FQ = [AircraftFilter("733")]
    F_FY = [AircraftFilter("883")]
    F_FI = [AircraftFilter("1035")]
    F_DG = [AircraftFilter("952")]
    F_TOW = [AircraftFilter("TOW")]
    F_1037 = [AircraftFilter("1037")]
    F_1037_OPEALE = [AircraftFilter("1037-opeale")]

    F_MOTTI = F_TOW + F_1037
    F_PURTSIKKA = F_FK + F_FM + F_FQ + F_FY + F_FI + F_DG

    F_KAIKKI_KONEET = F_MOTTI + F_PURTSIKKA
    F_LASKUTUSLISA = [InvoicingChargeFilter()]

    F_TRANSFER_TOW = [TransferTowFilter()]

    rules = [
        # OH-TOW
        FirstRule([
            # Nuorisoalennus + siirtohinaus
            MinimumDurationRule(
                FlightRule(122 * 0.75, ACCT_TOWING, 
                          F_TOW + F_MOTOR_PERIOD + F_TRANSFER_TOW + F_YOUTH,
                          "Siirtohinaus, TOW (nuorisoalennus), %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)"),
            
            # Nuorisoalennus
            MinimumDurationRule(
                FlightRule(122 * 0.75, ACCT_TOW,
                          F_TOW + F_MOTOR_PERIOD + F_YOUTH,
                          "Lento, TOW (nuorisoalennus), %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)"),
            
            # Siirtohinaus
            MinimumDurationRule(
                FlightRule(122, ACCT_TOWING,
                          F_TOW + F_MOTOR_PERIOD + F_TRANSFER_TOW,
                          "Siirtohinaus, TOW, %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)"),
            
            # Normaalilento
            MinimumDurationRule(
                FlightRule(122, ACCT_TOW,
                          F_TOW + F_MOTOR_PERIOD,
                          "Lento, TOW, %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)")
        ]),

        # OH-1037
        FirstRule([
            # Nuorisoalennus
            MinimumDurationRule(
                FlightRule(113 * 0.75, ACCT_1037,
                          F_1037 + F_MOTOR_PERIOD + F_YOUTH,
                          "Lento, 1037 (nuorisoalennus), %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)"),
            
            # Normaalilento
            MinimumDurationRule(
                FlightRule(113, ACCT_1037,
                          F_1037 + F_MOTOR_PERIOD,
                          "Lento, 1037, %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)")
        ]),

        # OH-1037 opeale
        FlightRule(65, ACCT_1037_OPEALE, F_1037_OPEALE + F_MOTOR_PERIOD, "Lento (opealennus), %(duration)d min"),

        # Purtsikat
        CappedRule(ID_PURSI_CAP_2024, 1250, ctx,
        AllRules([
            # Purtsikat
            FirstRule([
                FlightRule(18 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FK + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(18 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FK + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(18, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FK)
            ]),
            FirstRule([
                FlightRule(26 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FM + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(26 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FM + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(26, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FM)
            ]),
            FirstRule([
                FlightRule(28 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FQ + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(28 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FQ + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(28, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FQ)
            ]),
            FirstRule([
                FlightRule(29 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FI + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(29 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FI + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(29, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FI)
            ]),
            FirstRule([
                FlightRule(36 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FY + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(36 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FY + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(36, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FY)
            ]),
            FirstRule([
                FlightRule(44 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_DG + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(44 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_DG + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(44, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_DG)
            ])
        ])),

        # Koululentomaksu
        FlightRule(lambda ev: 6, ACCT_PURSI_INSTRUCTION, F_PURTSIKKA + F_GLIDER_SEASON + [PurposeFilter("KOU")], "Koululentomaksu, %(aircraft)s"),

        # Kalustomaksu
        CappedRule(ID_KALUSTOMAKSU_CAP_2024, 90, ctx,
                   AllRules([FlightRule(10, ACCT_KALUSTO, F_GLIDER_SEASON + F_PURTSIKKA,
                                    "Kalustomaksu, %(aircraft)s, %(duration)d min"),
                            FlightRule(10, ACCT_KALUSTO, F_GLIDER_SEASON + F_MOTTI,
                                    "Kalustomaksu, %(aircraft)s, %(duration)d min")])),

        # Normal simple events
        FirstRule([
            SimpleRule(F_FULL_YEAR + [PositivePriceFilter()]),
            SimpleRule(F_FULL_YEAR + [NegativePriceFilter()])
        ]),

        FlightRule(lambda ev: 2, ACCT_LASKUTUSLISA, F_KAIKKI_KONEET + F_GLIDER_SEASON + F_LASKUTUSLISA, "Laskutuslisä, %(aircraft)s, %(invoicing_comment)s")
    ]
    
    return [SetLedgerYearRule(AllRules(rules), YEAR)]

def events_to_lines(events, rules):
    skipped_accounts = set()
    for event in events:
        # Skip prefixed accounts before attempting to match rules
        if any(event.account_id.upper().startswith(prefix) for prefix in conf['no_invoicing_prefix']):
            skipped_accounts.add(event.account_id)
            continue
            
        match = False
        for rule in rules:
            for line in rule.invoice(event):
                match = True
                yield line
        if not match:
            print("No match for event", event.__repr__(), file=sys.stderr)
    
    if skipped_accounts:
        print("\nSkipped accounts:", ", ".join(sorted(skipped_accounts)), file=sys.stderr)

def grouped_lines(lines):
    by_account = defaultdict(lambda: [])
    for line in lines:
        by_account[line.account_id].append(line)
    return by_account

def events_to_invoices(events, rules, invoice_date=dt.date.today()):
    by_account = grouped_lines(events_to_lines(events, rules))
    for account in sorted(by_account.keys()):
        lines = sorted(by_account[account], key=lambda line: line.date)
        yield Invoice(account, invoice_date, lines)


def write_invoices_to_files(invoices, conf):
    out_dir = conf["out_dir"]
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    invoice_format_id = conf.get("invoice_format", "2015")
    for invoice in invoices:
        account = invoice.account_id
        with open(os.path.join(out_dir, account + ".txt"), "wb") as f:
            f.write(format_invoice(invoice, conf["description"], invoice_format_id).encode("utf-8"))

def write_hansa_export_file(valid_invoices, invalid_invoices, conf):
    out_dir = conf["out_dir"]
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    dates = list(map(parse_iso8601_date, conf['hansa_txn_dates']))
    hansa_txn_date_filter = PeriodFilter(Period(*dates))

    hansa_txns = []
    hansa_txn_id_gen = count(conf["hansa_first_txn_id"])
    for invoice in invoices:
        DEBUG = invoice.account_id == "114983"
        lines_by_rule = defaultdict(lambda: [])
        for line in invoice.lines:
            if hansa_txn_date_filter(line):
                lines_by_rule[line.rule].append(line)
            elif DEBUG:
                print("Discarding line because of date filter: %s" %line, file=sys.stderr)

        for (rule, lineset) in lines_by_rule.items():
            # Check all lines have same sign
            signs = [math.copysign(1, line.price) for line in lineset]

            if not (all(sign >= 0 for sign in signs) or all(sign <= 0 for sign in signs)):
                
                print("\n-------------")
                for line_item in lineset:
                    print((line_item.item.encode("utf-8") + ": " + str(line_item.price)))
                    
                print("Inconsistent signs:", (str(item.to_json()) for item in lineset), signs, all(sign >= 0 for sign in signs), all(sign <= 0 for sign in signs), file=sys.stderr)

            # Check all lines have same ledger account, excluding lines that don't go
            # into ledger via this process (they have None as ledger_account_id)
            ledger_accounts = set(line.ledger_account_id for line in lineset) - set([None])
            if len(ledger_accounts) > 1 and not rule.allow_multiple_ledger_categories:
                print("Inconsistent ledger accounts:", ", ".join(str(l) for l in lineset), ledger_accounts, file=sys.stderr)
                
        hansa_rows = []
        for lineset in list(lines_by_rule.values()):
            extract_lai = lambda x: x.ledger_account_id
            for (ledger_account_id, lines) in groupby(sorted(lineset, key=extract_lai), key=extract_lai):
                lines = list(lines)
                if DEBUG:
                    print("Ledger account id:", ledger_account_id, len(lines), file=sys.stderr)
                lines = list(lines)
                if not ledger_account_id:
                    if DEBUG:
                        print("Not going into Hansa:", file=sys.stderr)
                        for line in lines:
                            print(str(line), file=sys.stderr)
                    continue
            
                total_price = sum(line.price for line in lines if line.ledger_account_id)
                if total_price == 0:
                    if DEBUG:
                        print("Not writing hansa line for zero-sum line on account", ledger_account_id, file=sys.stderr)
                    continue
            
                title = os.path.commonprefix([line.item for line in lines])
                if DEBUG:
                    print("Writing hansa line for account", ledger_account_id, "->", total_price, file=sys.stderr)
                if total_price > 0:
                    member_line = SimpleHansaRow(1422, title, debit=total_price)
                    club_line = SimpleHansaRow(ledger_account_id, title, credit=total_price)
                else:
                    member_line = SimpleHansaRow(1422, title, credit=total_price)
                    club_line = SimpleHansaRow(ledger_account_id, title, debit=total_price)
                hansa_rows.append(club_line)
                hansa_rows.append(member_line)

        hansa_rows.sort()

        if hansa_rows:
            hansa_id = next(hansa_txn_id_gen)
            hansa_txn = SimpleHansaTransaction(hansa_id, conf["hansa_year"], conf["hansa_entry_date"], conf["hansa_txn_date"], "Lentolasku, " + invoice.account_id, invoice.account_id, hansa_rows)
            hansa_txns.append(hansa_txn)

    with open(os.path.join(out_dir, "hansa-export-" + conf["invoice_date"] + ".txt"), "wb") as f:
        for txn in hansa_txns:
            f.write(unicodedata.normalize("NFC", txn.hansaformat()).encode("iso-8859-15"))

def write_total_csv(invoices, fname):
    import csv
    writer = csv.writer(open(fname, 'wb'))
    writer.writerows(invoice.to_csvrow_total() for invoice in invoices)

def write_row_csv(invoices, fname_template):
    import unicodecsv
    by_year = defaultdict(lambda: [])
    for invoice in invoices:
        for line in invoice.lines:
            if not line.rollup:
                row = line.to_csvrow()
                by_year[row.ledger_year].append(row)
    for year, yearly_rowset in by_year.items():
        writer = unicodecsv.writer(open(fname_template%year, 'wb'), encoding='utf-8')
        writer.writerows(yearly_rowset)

def is_invoice_zero(invoice):
    return abs(invoice.total()) < 0.01

def make_event_validator(pik_ids, external_ids):
    def event_validator(event):
        if not isinstance(event.account_id, str):
            raise ValueError("Account id must be string, was: " + repr(event.account_id) + " in " + str(event))
        if not ((event.account_id in pik_ids and len(event.account_id) in (4,6)) or
                event.account_id in external_ids):
            raise ValueError("Invalid id was: " + repr(event.account_id) + " in " + str(event))
        return event
    return event_validator

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)

def read_pik_ids(fnames):
    result = []
    for fname in fnames:
        result.extend(x.strip() for x in open(fname, 'r', encoding='utf-8').readlines() if x.strip())
    return result

def read_birth_dates(fnames):
    """Read birth dates from files in format: account_id,birth_date where date is DD.MM.YYYY"""
    result = {}
    for fname in fnames:
        with open(fname, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if row and not row[0].startswith('#'):  # Skip empty lines and comments
                    account_id, birth_date = row[0].strip(), row[1].strip()
                    # Convert DD.MM.YYYY to YYYY-MM-DD
                    if birth_date and '.' in birth_date:
                        day, month, year = birth_date.split('.')
                        birth_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    result[account_id] = birth_date
    return result

def read_member_ids(fnames):
    """Read member IDs from CSV files"""
    result = set()
    for fname in fnames:
        with open(fname, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if row and not row[0].startswith('#'):  # Skip empty lines and comments
                    result.add(row[0].strip())
    return result

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: invoice-flights.py <conf-file>")
        sys.exit(1)
    conf = json.load(open(sys.argv[1], 'r', encoding='utf-8'))

    sources = []

    ctx = BillingContext()
    if "context_file_in" in conf:
        context_file = conf["context_file_in"]
        if os.path.isfile(context_file):
            ctx = BillingContext.from_json(json.load(open(context_file, "r"), parse_float=decimal.Decimal))
    
    # Read birth dates if specified in config
    birth_dates = {}
    if 'birth_date_files' in conf:
        birth_dates = read_birth_dates(conf['birth_date_files'])

    # Read course member IDs if specified in config
    course_members = set()
    if 'course_member_files' in conf:
        course_members = read_member_ids(conf['course_member_files'])

    rules = make_rules(ctx, metadata={
        "birth_dates": birth_dates,
        "course_members": course_members
    })

    for fname in conf['event_files']:
        reader = csv.reader(open(fname, 'r', encoding='utf-8'))
        sources.append(SimpleEvent.generate_from_csv(reader))

    for fname in conf['flight_files']:
        reader = csv.reader(open(fname, 'r', encoding='utf-8'))
        sources.append(Flight.generate_from_csv(reader))

    for fname in conf['nda_files']:
        bank_txn_date_filter = lambda txn_date: True
        if 'bank_txn_dates' in conf:
            dates = list(map(parse_iso8601_date, conf['bank_txn_dates']))
            bank_txn_date_filter = PeriodFilter(Period(*dates))

        reader = nda.transactions(open(fname, 'r', encoding='utf-8'))
        # Only PIK references and incoming transactions - note that the conversion reverses the sign of the sum, since incoming money reduces the account's debt
        sources.append(SimpleEvent.generate_from_nda(reader, ["FI2413093000112458"], lambda event: bank_txn_date_filter(event) and event.cents > 0 and event.ref and (len(event.ref) == 4 or len(event.ref) == 6)))

    invoice_date = parse_iso8601_date(conf['invoice_date'])
    event_validator = make_event_validator(read_pik_ids(conf['valid_id_files']), conf['no_invoicing_prefix'])
    events = list(sorted(chain(*sources), key=lambda event: event.date))
    for event in events:
        try:
            event_validator(event)
        except ValueError as e:
            print("Invalid account id", event.account_id, str(event), file=sys.stderr)

    invoices = list(events_to_invoices(events, rules, invoice_date=invoice_date))

    valid_invoices = [i for i in invoices if not is_invoice_zero(i)]
    invalid_invoices = [i for i in invoices if is_invoice_zero(i)]

    out_dir = conf["out_dir"]
    if os.path.exists(out_dir):
        raise ValueError("out_dir already exists: " + out_dir)

    total_csv_fname = conf.get("total_csv_name", os.path.join(out_dir, "totals.csv"))
    row_csv_fname_template = conf.get("row_csv_name_template", os.path.join(out_dir, "rows_%s.csv"))

    write_invoices_to_files(valid_invoices, conf)
    write_invoices_to_files(invalid_invoices, conf)
    write_hansa_export_file(valid_invoices, invalid_invoices, conf)
    write_total_csv(invoices, total_csv_fname)
    write_row_csv(invoices, row_csv_fname_template)
    if "context_file_out" in conf:
        json.dump(ctx.to_json(), open(conf["context_file_out"], "w"), cls=DecimalEncoder)

    machine_readable_invoices = [invoice.to_json() for invoice in invoices]

    #print json.dumps(machine_readable_invoices)

    invalid_account = []
    invalid_sum = []

    print("Difference, valid invoices, total", sum(i.total() for i in valid_invoices), file=sys.stderr)
    print("Owed to club, invoices, total", sum(i.total() for i in valid_invoices if i.total() > 0), file=sys.stderr)
    print("Owed by club, invoices, total", sum(i.total() for i in valid_invoices if i.total() < 0), file=sys.stderr)

    print("Zero invoices, count ", len(invalid_invoices), file=sys.stderr)

