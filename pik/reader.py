from pik.event import SimpleEvent
from pik.util import Period, parse_iso8601_date
from pik.flights import Flight
from pik.rules import PeriodFilter
from pik.billing import BillingContext
from itertools import chain

import pik.nda as nda

import csv
import os
import json
import decimal

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

def load_configuration(conf_file):
    """Load configuration from Python or JSON file"""
    if conf_file.endswith('.json'):
        with open(conf_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    elif conf_file.endswith('.py'):
        import importlib.util
        import sys
        
        # Add the config file's directory to Python path
        sys.path.insert(0, os.path.dirname(os.path.abspath(conf_file)))
        
        # Load the config file as a module
        spec = importlib.util.spec_from_file_location("config", conf_file)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
        
        # Convert module attributes to dict
        conf = {k: v for k, v in vars(config).items() 
               if not k.startswith('__') and not callable(v)}
        
        return conf
    else:
        raise ValueError("Configuration file must end with .json or .py")

def load_billing_context(conf):
    """Load billing context from configuration"""
    ctx = BillingContext()
    if "context_file_in" in conf:
        context_file = conf["context_file_in"]
        if os.path.isfile(context_file):
            with open(context_file, "r") as f:
                ctx = BillingContext.from_json(json.load(f, parse_float=decimal.Decimal))
    return ctx

def load_metadata(conf):
    """Load birth dates and course members from configuration"""
    metadata = {
        "birth_dates": {},
        "course_members": set()
    }
    
    if 'birth_date_files' in conf:
        metadata["birth_dates"] = read_birth_dates(conf['birth_date_files'])
    
    if 'course_member_files' in conf:
        metadata["course_members"] = read_member_ids(conf['course_member_files'])
        
    return metadata

def load_events(conf):
    """Load all events from configured sources"""
    sources = []
    
    # Load simple events
    for fname in conf['event_files']:
        with open(fname, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            sources.append(list(SimpleEvent.generate_from_csv(reader)))

    # Load flight events  
    for fname in conf['flight_files']:
        with open(fname, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            sources.append(list(Flight.generate_from_csv(reader)))

    # Load NDA transactions
    for fname in conf['nda_files']:
        bank_txn_date_filter = lambda txn_date: True
        if 'bank_txn_dates' in conf:
            dates = list(map(parse_iso8601_date, conf['bank_txn_dates']))
            bank_txn_date_filter = PeriodFilter(Period(*dates))

        with open(fname, 'r', encoding='utf-8') as f:
            reader = nda.transactions(f)
            sources.append(list(SimpleEvent.generate_from_nda(
                reader,
                ["FI2413093000112458"], 
                lambda event: bank_txn_date_filter(event) and event.cents > 0 and event.ref and (len(event.ref) in (4,6))
            )))

    # Flatten and sort all events
    return sorted(chain(*sources), key=lambda event: event.date)