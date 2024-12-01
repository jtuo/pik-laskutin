# -*- coding: utf-8

# All rules must have:
#
# "invoice" method, which takes: a source event, and produces a list of pik.billing.InvoiceLine objects

from pik.event import SimpleEvent
from pik.flights import Flight
from pik.billing import InvoiceLine
import datetime as dt
import re
import numbers
import sys
import decimal

class BaseRule(object):
    # Don't allow multiple ledger accounts for lines produced by a rule by default
    allow_multiple_ledger_accounts = False

class DebugRule(BaseRule):
    def __init__(self, inner_rule, debug_filter=lambda event, result: bool(result), debug_func=lambda ev, result: sys.stdout.write(str(ev) + " " + str(result) + "\n")):
        self.inner_rule = inner_rule
        self.debug_filter = debug_filter
        self.debug_func = debug_func

    def invoice(self, event):
        result = self.inner_rule.invoice(event)
        do_debug = self.debug_filter(event, result)
        if do_debug:
            self.debug_func(event, result)
        return result

class SimpleRule(BaseRule):
    """
    Simple rule for SimpleEvents

    Matches a SimpleEvent that matches all the filters.

    Creates a single InvoiceLine for the event.
    """
    def __init__(self, filters=[]):
        self.filters = filters
        # Allow multiple ledger accounts for lines produced by this rule, since the category comes from the source event
        self.allow_multiple_ledger_categories = True

    def invoice(self, event):
        if isinstance(event, SimpleEvent):
            if all(f(event) for f in self.filters):
                return [InvoiceLine(event.account_id, event.date, event.item, event.amount, self, event, event.ledger_account_id, event.ledger_year, event.rollup)]
        return []

class SinceDateFilter(object):
    """
    Match events on or after the date stored in given variable in given context.

    Date must be stored in ISO 8601 format (yyyy-mm-dd)
    """
    def __init__(self, ctx, variable_id):
        self.ctx = ctx
        self.variable_id = variable_id

    def __call__(self, event):
        try:
            val = self.ctx.get(event.account_id, self.variable_id)
            limit = dt.date(*list(map(int, val.split("-"))))
            return limit <= event.date
        except Exception:
            return False

def flightFilter(ev):
    """
    Match events of type Flight
    """
    return isinstance(ev, Flight)

def eventFilter(ev):
    """
    Match events of type SimpleEvent
    """
    return isinstance(ev, SimpleEvent)

class ItemFilter(object):
    """
    Match events whose 'item' property matches given regexp.
    """
    def __init__(self, regex):
        self.regex = regex

    def __call__(self, event):
        return re.search(self.regex, event.item)

class PeriodFilter(object):
    """
    Match events in given period
    """
    def __init__(self, period):
        """
        :param period: period to match
        :type period: pik.util.Period
        """
        self.period = period

    def __call__(self, event):
        return event.date in self.period

class AircraftFilter(object):
    """
    Match (Flight) events with one of given aircraft
    """
    def __init__(self, *aircraft):
        self.aircraft = aircraft

    def __call__(self, event):
        return event.aircraft in self.aircraft

class PurposeFilter(object):
    """
    Match (Flight) events with one of given purposes of flight
    """
    def __init__(self, *purposes):
        self.purposes = purposes

    def __call__(self, event):
        return event.purpose in self.purposes

class NegationFilter(object):
    """
    Match events that don't match given filter
    """
    def __init__(self, filter):
        self.filter = filter

    def __call__(self, event):
        return not self.filter(event)

class TransferTowFilter(object):
    """
    Match (Flight) events with transfer_tow property
    """
    def __call__(self, event):
        return bool(event.transfer_tow)

class InvoicingChargeFilter(object):
    """
    Match (Flight) events with invoicing_comment set (indicates invoicing surcharge should be added)
    """
    def __call__(self, event):
        return bool(event.invoicing_comment)

class PositivePriceFilter(object):
    """
    Match SimpleEvents with price 0 or greater
    """
    def __call__(self, event):
        return event.amount >= 0

class NegativePriceFilter(object):
    """
    Match SimpleEvents with price less than 0
    """
    def __call__(self, event):
        return event.amount < 0

class BirthDateFilter(object):
    """
    Match events where the pilot's age at flight time is within given range
    """
    def __init__(self, birth_dates, max_age):
        self.birth_dates = birth_dates
        self.max_age = max_age

    def __call__(self, event):
        try:
            birth_date_str = self.birth_dates.get(event.account_id)
            if not birth_date_str:
                return False
            birth_date = dt.date(*map(int, birth_date_str.split("-")))
            age_at_flight = (event.date - birth_date).days / 365.25
            return age_at_flight <= self.max_age
        except:
            return False

class OrFilter(object):
    """
    Match if any of the given filters match
    """
    def __init__(self, filters):
        """
        :param filters: List of filters to check
        """
        self.filters = []
        for filter_list in filters:
            if isinstance(filter_list, list):
                # If the list contains an OrFilter, add its filters
                if len(filter_list) == 1 and isinstance(filter_list[0], OrFilter):
                    self.filters.extend(filter_list[0].filters)
                # If it's a list containing filters, add the first one
                else:
                    self.filters.append(filter_list[0])
            else:
                self.filters.append(filter_list)

    def __call__(self, event):
        return any(f(event) for f in self.filters)

class MemberListFilter(object):
    """
    Match events based on member reference IDs (PIK viite) using either whitelist or blacklist mode
    """
    def __init__(self, member_ids, whitelist_mode=True):
        """
        :param member_ids: Set/list of member reference IDs to match against
        :param whitelist_mode: If True, match members IN the list. If False, match members NOT in the list
        """
        self.member_ids = set(str(id) for id in member_ids)  # Convert all IDs to strings for consistency
        self.whitelist_mode = whitelist_mode

    def __call__(self, event):
        member_id = str(event.account_id)
        if self.whitelist_mode:
            return member_id in self.member_ids
        else:
            return member_id not in self.member_ids

class MinimumDurationRule(BaseRule):
    """
    Apply minimum duration billing to flights
    """
    def __init__(self, inner_rule, aircraft_filters, min_duration, min_duration_text=None):
        """
        :param inner_rule: The rule to wrap
        :param aircraft_filters: List of aircraft filters to check if minimum billing applies
        :param min_duration: Minimum duration to bill in minutes (required)
        :param min_duration_text: Text to append to description when minimum billing applies
        """
        self.inner_rule = inner_rule
        self.aircraft_filters = aircraft_filters
        self.min_duration = min_duration
        self.min_duration_text = min_duration_text

    def invoice(self, event):
        if isinstance(event, Flight):
            # Store original duration
            orig_duration = event.duration
            # Check if minimum billing applies
            applies = (any(f(event) for f in self.aircraft_filters) and 
                      not event.transfer_tow and 
                      event.duration < self.min_duration)
            
            if applies:
                # Temporarily modify duration
                event.duration = self.min_duration
            
            # Get invoice lines
            lines = self.inner_rule.invoice(event)
            
            # Restore original duration
            event.duration = orig_duration
            
            # Add minimum duration text if applicable
            if applies and self.min_duration_text and lines:
                for line in lines:
                    line.item = line.item + " " + self.min_duration_text
            
            return lines
        return self.inner_rule.invoice(event)

class FlightRule(BaseRule):
    """
    Produce one InvoiceLine from a Flight event if it matches all the
    filters, priced with given price, and with description derived from given template.
    """
    def __init__(self, price, ledger_account_id, filters=[], template="Lento, %(aircraft)s, %(duration)d min"):
        """
        :param price: Hourly price, in euros, or pricing function that takes Flight event as parameter and returns price
        :param ledger_account_id: Ledger account id of the other side of the transaction (income account)
        :param filters: Input filters (such as per-aircraft)
        :param template: Description tmeplate. Filled using string formatting with the event object's __dict__ context
        """
        if isinstance(price, numbers.Number):
            self.pricing = lambda event: event.duration * (price / 60.0)
        else:
            self.pricing = price
        self.filters = filters
        self.template = template
        self.ledger_account_id = ledger_account_id

    def invoice(self, event):
        if isinstance(event, Flight):
            if all(f(event) for f in self.filters):
                line = self.template %event.__dict__
                price = self.pricing(event)
                return [InvoiceLine(event.account_id, event.date, line, price, self, event, self.ledger_account_id)]
            
        return []

class AllRules(BaseRule):
    """
    Apply all given rules, and return InvoiceLines produced by all of them
    """
    def __init__(self, inner_rules):
        """
        :param inner_rules: Apply all inner rules to the incoming event and gather their InvoiceLines into the output
        """
        self.inner_rules = inner_rules

    def invoice(self, event):
        result = []
        for rule in self.inner_rules:
            result.extend(rule.invoice(event))
        return result

class FirstRule(BaseRule):
    """
    Apply given rules until a rule produces an InvoiceLine, result is that line
    """
    def __init__(self, inner_rules):
        """
        :param inner_rules: Apply inner rules in order, return with lines from first rule that produces output
        """
        self.inner_rules = inner_rules

    def invoice(self, event):
        for rule in self.inner_rules:
            lines = rule.invoice(event)
            if lines:
                return lines
        return []

class CappedRule(BaseRule):
    """
    Context-sensitive capped pricing rule

    1. Retrieve value of variable from context
    2. Apply inner rules to the event
    3. Filter resulting invoice lines so that:
      - if context value is already at or over cap, drop line
      - if context value + line value is over cap, modify the line so that context value + modified line value is at cap value, add modified line to context value, and pass through modified line
      - else add line value to context value, and pass through line
    """
    def __init__(self, variable_id, cap_price, context, inner_rule):
        """
        :param variable_id: Variable to use for capping
        :param inner_rule: Rule that produces InvoiceLines that this object filters
        :param cap_price: Hourly price, in euros
        :param context: Billing context in which to store cap data
        """
        self.variable_id = variable_id
        self.inner_rule = inner_rule
        self.cap_price = decimal.Decimal(cap_price)
        self.context = context

    def invoice(self, event):
        lines = self.inner_rule.invoice(event)
        return list(self._filter_lines(lines))
    
    def _filter_lines(self, lines):
        for line in lines:
            ctx_val = self.context.get(line.account_id, self.variable_id)
            if ctx_val >= self.cap_price:
                # Already over cap, filter lines out
                continue
            if ctx_val + line.price > self.cap_price:
                # Cap price of line to match cap
                line = InvoiceLine(line.account_id, line.date, line.item + ", rajattu", self.cap_price - ctx_val, self, line.event, line.ledger_account_id)
            self.context.set(line.account_id, self.variable_id, ctx_val + line.price)
            yield line

class SetDateRule(BaseRule):
    """
    Rule that sets a context variable to date of last line produced by inner rule
    """
    def __init__(self, variable_id, context, inner_rule):
        self.variable_id = variable_id
        self.inner_rule = inner_rule
        self.context = context

    def invoice(self, event):
        lines = self.inner_rule.invoice(event)
        for line in lines:
            self.context.set(line.account_id, self.variable_id, line.date.isoformat())
        return lines

class SetLedgerYearRule(BaseRule):
    """
    Rule that writes given ledger year into output InvoiceLines if it's not set
    """
    def __init__(self, inner_rule, ledger_year):
        self.inner_rule = inner_rule
        self.ledger_year = ledger_year

    def invoice(self, event):
        lines = self.inner_rule.invoice(event)
        for line in lines:
            if line.ledger_year is None:
                line.ledger_year = self.ledger_year
        return lines
