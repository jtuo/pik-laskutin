from decimal import Decimal
from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from .models import BaseEvent, Account, AccountEntry
from .billing import BillingContext
from .logic import make_rules
from .processor import load_metadata
from loguru import logger

class RuleEngine:
    """Main rule engine that applies all registered rules"""
    
    def __init__(self):
        self.rules = []
    
    def add_rules(self, rules):
        """Add multiple rules to the engine"""
        self.rules.extend(rules)

    def process_event(self, event: BaseEvent, session: Session) -> List:
        """
        Process an event through all registered rules
        """
        logger.debug(f"Processing event: {event}")
        event = session.merge(event)
        session.flush()
        
        lines = []
        for rule in self.rules:
            new_lines = rule.invoice(event)
            
            for line in new_lines:
                if isinstance(line, AccountEntry):
                    # Add the entry to the session first
                    session.add(line)
                    # Ensure it's associated with the event
                    if line not in event.account_entries:
                        event.account_entries.append(line)
                
                lines.append(line)
                session.flush()
        
        session.flush()
        return lines

    def process_events(self, events: List[BaseEvent], session: Session) -> Dict[Account, List]:
        """
        Process multiple events and group invoice lines by account
        
        Args:
            events: List of events to process
            session: SQLAlchemy session
            
        Returns:
            Dict[Account, List[InvoiceLine]]: Invoice lines grouped by account
        """
        results: Dict[Account, List] = {}
        
        for event in events:
            # Ensure event is attached to session and flush any pending changes
            event = session.merge(event)
            session.flush()
            
            # Ensure account is loaded and attached to session before processing
            account = session.merge(event.account)
            session.flush()
            
            lines = self.process_event(event, session)
            if lines:
                if account not in results:
                    results[account] = []
                results[account].extend(lines)
                
                # Ensure all account entries are properly attached
                for entry in event.account_entries:
                    if entry not in session:
                        session.add(entry)
                session.flush()
        
        return results

def create_default_engine() -> RuleEngine:
    """Create a RuleEngine with default rules"""
    engine = RuleEngine()

    context = BillingContext()
    metadata = load_metadata("V:/bookkeeping_2024-12.json")
    engine.add_rules(make_rules(context, metadata=metadata))
    
    return engine
