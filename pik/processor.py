import logging
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
    logger = logging.getLogger('pik.processor')
    skipped_accounts = set()
    for event in events:
        # Skip prefixed accounts before attempting to match rules
        if any(event.account_id.upper().startswith(prefix) for prefix in config['no_invoicing_prefix']):
            skipped_accounts.add(event.account_id)
            continue
            
        match = False
        for rule in rules:
            lines = list(rule.invoice(event))
            if lines:
                match = True
                for line in lines:
                    yield line
        if not match:
            logger.warning("No match for event %s", event.__repr__())
    
    if skipped_accounts:
        logging.getLogger('pik.processor').info("Skipped accounts: %s", ", ".join(sorted(skipped_accounts)))

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
    logger = logging.getLogger('pik.processor')
    total_count = sum(invalid_counts.values())
    if total_count > 0:
        logger.warning("Summary of invalid events:")
        logger.warning("-" * 40)
        for event_type in sorted(invalid_counts.keys()):
            count = invalid_counts[event_type]
            total = invalid_totals[event_type]
            if event_type == 'SimpleEvent':
                logger.warning(f"{event_type}s: {count} events, total amount: €{total:.2f}")
            else:
                logger.warning(f"{event_type}s: {count} events")
        logger.warning("-" * 40)
        logger.warning(f"Total invalid events: {total_count}")
    else:
        logger.info("All events were accounted for.")

def print_summary(valid_invoices, invalid_invoices):
    logger = logging.getLogger('pik.processor')
    logger.info("Summary of invoices:")
    logger.info("-" * 40)
    logger.info("Invoices written: %d", len(valid_invoices))
    logger.info("Zero invoices, count %d", len(invalid_invoices))
    logger.info("Owed to club, invoices, total %s", sum(i.total() for i in valid_invoices if i.total() > 0))
    logger.info("Owed by club, invoices, total %s", sum(i.total() for i in valid_invoices if i.total() < 0))
    logger.info("Difference, valid invoices, total %s", sum(i.total() for i in valid_invoices))
    logger.info("-" * 40)

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
