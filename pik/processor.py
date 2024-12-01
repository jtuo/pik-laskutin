from pik.util import parse_iso8601_date, DecimalEncoder
from pik.billing import Invoice
from pik.validation import validate_events
from pik.writer import write_outputs
from pik.reader import load_billing_context, load_metadata, load_events

from collections import defaultdict

import datetime as dt
import sys
import json

def events_to_lines(events, rules, config):
    skipped_accounts = set()
    for event in events:
        # Skip prefixed accounts before attempting to match rules
        if any(event.account_id.upper().startswith(prefix) for prefix in config['no_invoicing_prefix']):
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

def events_to_invoices(events, rules, config, invoice_date=dt.date.today()):
    by_account = grouped_lines(events_to_lines(events, rules, config))
    for account in sorted(by_account.keys()):
        lines = sorted(by_account[account], key=lambda line: line.date)
        yield Invoice(account, invoice_date, lines)

def print_validation_summary(invalid_counts, invalid_totals):
    total_count = sum(invalid_counts.values())
    if total_count > 0:
        print("\nSummary of invalid events:", file=sys.stderr)
        print("-" * 40, file=sys.stderr)
        for event_type in sorted(invalid_counts.keys()):
            count = invalid_counts[event_type]
            total = invalid_totals[event_type]
            if event_type == 'SimpleEvent':
                print(f"{event_type}s: {count} events, total amount: €{total:.2f}", file=sys.stderr)
            else:
                print(f"{event_type}s: {count} events", file=sys.stderr)
        print("-" * 40, file=sys.stderr)
        print(f"Total invalid events: {total_count}", file=sys.stderr)
    else:
        print("\nAll events were accounted for.", file=sys.stderr)

def print_summary(valid_invoices, invalid_invoices):
    print("Zero invoices, count ", len(invalid_invoices), file=sys.stderr)
    print("Owed to club, invoices, total", sum(i.total() for i in valid_invoices if i.total() > 0), file=sys.stderr)
    print("Owed by club, invoices, total", sum(i.total() for i in valid_invoices if i.total() < 0), file=sys.stderr)
    print("Difference, valid invoices, total", sum(i.total() for i in valid_invoices), file=sys.stderr)

def save_context(ctx, config):
    if "context_file_out" in config:
        with open(config["context_file_out"], "w") as f:
            json.dump(ctx.to_json(), f, cls=DecimalEncoder)

def process_billing(config, make_rules):
    # Load configuration and setup
    ctx = load_billing_context(config)
    metadata = load_metadata(config)
    
    # Create rules and process
    rules = make_rules(ctx, metadata)
    events = load_events(config)
    
    # Validate
    invalid_counts, invalid_totals = validate_events(events, config)
    print_validation_summary(invalid_counts, invalid_totals)
    
    # Generate and write invoices
    invoice_date = parse_iso8601_date(config['invoice_date'])
    invoices = list(events_to_invoices(events, rules, config, invoice_date=invoice_date))
    valid_invoices, invalid_invoices = write_outputs(invoices, config)
    
    # Save context if configured
    save_context(ctx, config)
    print_summary(valid_invoices, invalid_invoices)
