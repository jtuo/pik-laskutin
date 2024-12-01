from pik.reader import read_pik_ids
import sys
from collections import defaultdict
import decimal
from pik.event import SimpleEvent

def make_event_validator(pik_ids, external_ids):
    def event_validator(event):
        if not isinstance(event.account_id, str):
            raise ValueError("Account id must be string, was: " + repr(event.account_id) + " in " + str(event))
        if not ((event.account_id in pik_ids and len(event.account_id) in (4,6)) or
                event.account_id in external_ids):
            raise ValueError("Invalid id was: " + repr(event.account_id) + " in " + str(event))
        return event
    return event_validator

def validate_events(events, conf):
    """Validate events and return validation report"""
    pik_ids = read_pik_ids(conf['valid_id_files'])
    validator = make_event_validator(pik_ids, conf['no_invoicing_prefix'])
    
    invalid_counts = defaultdict(int)
    invalid_totals = defaultdict(decimal.Decimal)
    
    for event in events:
        try:
            validator(event)
        except ValueError as e:
            print("Invalid account id", event.account_id, str(event), file=sys.stderr)
            event_type = event.__class__.__name__
            invalid_counts[event_type] += 1
            if isinstance(event, SimpleEvent):
                invalid_totals[event_type] += decimal.Decimal(str(event.amount))
    
    return invalid_counts, invalid_totals