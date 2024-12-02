from django.db import transaction
from operations.models import BaseEvent
from billing.models import Account, AccountEntry
from typing import List, Dict
from legacy.billing import BillingContext
from .logic import make_rules
from legacy.processor import load_metadata
from loguru import logger

class RuleEngine:
    def __init__(self):
        self.rules = []

    def add_rules(self, rules):
        self.rules.extend(rules)

    @transaction.atomic
    def process_event(self, event: BaseEvent) -> List:
        lines = []
        for rule in self.rules:
            new_lines = rule.invoice(event)
            for line in new_lines:
                if isinstance(line, AccountEntry):
                    line.save()
                    event.account_entries.add(line)
                lines.append(line)
        return lines

    @transaction.atomic
    def process_events(self, events: List[BaseEvent]) -> Dict[Account, List]:
        results: Dict[Account, List] = {}
        for event in events:
            account = event.account
            lines = self.process_event(event)
            if lines:
                if account not in results:
                    results[account] = []
                results[account].extend(lines)
        return results

def create_default_engine() -> RuleEngine:
    """Create a RuleEngine with default rules"""
    engine = RuleEngine()

    context = BillingContext()
    metadata = load_metadata("V:/bookkeeping_2024-12.json")
    engine.add_rules(make_rules(context, metadata=metadata))
    
    return engine
