from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, Numeric, Text, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, validates, backref
from datetime import datetime
import enum
from config import Config
from decimal import Decimal, ROUND_HALF_UP

# Treat SQLAlchemy warnings as exceptions
from sqlalchemy import exc as sa_exc
import warnings
warnings.filterwarnings('error', category=sa_exc.SAWarning)

Base = declarative_base()

class Member(Base):
    __tablename__ = 'members'
    
    id = Column(String(20), primary_key=True)  # PIK reference number
    name = Column(String(100), nullable=False)
    email = Column(String(255))
    birth_date = Column(Date, nullable=True)
    active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    accounts = relationship("Account", back_populates="member")

    @validates('id')
    def validate_id(self, key, value):
        if not value:
            raise ValueError("Reference ID cannot be empty")
        if not value.isdigit():
            raise ValueError("Reference ID must be a number")
        return value

    def __repr__(self):
        return f"<Member {self.id}: {self.name}>"


class Account(Base):
    __tablename__ = 'accounts'
    
    id = Column(String(20), primary_key=True)  # Using PIK reference as primary key
    member_id = Column(String(20), ForeignKey('members.id'), nullable=False)
    name = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    member = relationship("Member", back_populates="accounts")
    flights = relationship("Flight", back_populates="account")
    entries = relationship("AccountEntry", back_populates="account")
    invoices = relationship("Invoice", back_populates="account")

    def __repr__(self):
        return f"<Account {self.id}: {self.name}>"

class AccountEntry(Base):
    __tablename__ = 'account_entries'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False, index=True)
    account_id = Column(String(20), ForeignKey('accounts.id'), nullable=False)
    description = Column(Text, nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)  # Positive = charge, Negative = payment/credit
    force_balance = Column(Numeric(10, 2), nullable=True)  # If set, forces balance to this value
    event_id = Column(Integer, ForeignKey('events.id'), nullable=True)
    invoice_id = Column(Integer, ForeignKey('invoices.id'), nullable=True)
    ledger_account_id = Column(String(20), nullable=True)  # For mapping to external accounting system
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    account = relationship("Account", back_populates="entries")
    source_event = relationship(
        "BaseEvent",
        back_populates="account_entries",
        cascade="all"  # Remove delete-orphan if entries can exist without events
    )

    @validates('amount', 'force_balance')
    def validate_amounts(self, key, value):
        if value is None:
            return value
        return Decimal(str(value)).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)

    @property
    def is_modifiable(self):
        # If there's no source event, it's modifiable
        if not self.source_event:
            return True
        # Otherwise check the event type
        return self.source_event.type != 'invoice'

    @property
    def is_balance_correction(self):
        return self.force_balance is not None

    def __repr__(self):
        return f"<AccountEntry {self.date}: {self.amount}>"

class InvoiceStatus(enum.Enum):
    DRAFT = "draft"
    SENT = "sent"
    PAID = "paid"
    CANCELLED = "cancelled"

class Aircraft(Base):
    __tablename__ = 'aircraft'
    
    id = Column(Integer, primary_key=True)
    registration = Column(String(10), nullable=False, unique=True)
    competition_id = Column(String(4), nullable=True)
    name = Column(String(50), nullable=True)
    
    flights = relationship("Flight", back_populates="aircraft")

    @validates('registration')
    def validate_registration(self, key, value):
        if not value:
            raise ValueError("Registration cannot be empty")
        return value.upper()  # Store registrations in uppercase

    def __repr__(self):
        return f"<{self.registration}>"

class BaseEvent(Base):
    __tablename__ = 'events'
    
    id = Column(Integer, primary_key=True)
    account_id = Column(String(20), ForeignKey('accounts.id'), nullable=False)
    reference_id = Column(String(20), nullable=False, index=True)
    type = Column(String(50))

    date = Column(DateTime, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    account = relationship("Account")
    account_entries = relationship(
        "AccountEntry",
        back_populates="source_event",
        cascade="all"
    )

    __mapper_args__ = {
        'polymorphic_identity': 'event',
        'polymorphic_on': type
    }

    @validates('reference_id')
    def validate_reference_id(self, key, value):
        if not value:
            raise ValueError("Reference ID cannot be empty")
        
        # Skip further validation if this is a non-invoicing reference ID
        if value in Config.NO_INVOICING_REFERENCE_IDS:
            return value
            
        # If account_id is set, verify reference_id matches the account
        if self.account_id is not None:
            if value != self.account.reference_id:
                raise ValueError(f"Reference ID {value} does not match account's reference ID {self.account.reference_id}")
        
        return value

class Flight(BaseEvent):
    __tablename__ = 'flights'
    
    id = Column(Integer, ForeignKey('events.id'), primary_key=True)
    departure_time = Column(DateTime, nullable=False)
    landing_time = Column(DateTime, nullable=False)
    aircraft_id = Column(Integer, ForeignKey('aircraft.id'), nullable=False)
    duration = Column(Numeric(5, 2), nullable=False)
    purpose = Column(String(10), nullable=True)
    notes = Column(Text)
    
    aircraft = relationship("Aircraft", back_populates="flights")
    
    __mapper_args__ = {
        'polymorphic_identity': 'flight'
    }

    def __repr__(self):
        return f"<Flight {self.reference_id} on {self.date}>"

class Invoice(Base):
    __tablename__ = 'invoices'
    
    id = Column(Integer, primary_key=True)
    number = Column(String(20), unique=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    account_id = Column(String(20), ForeignKey('accounts.id'), nullable=False)
    due_date = Column(DateTime)
    status = Column(Enum(InvoiceStatus), default=InvoiceStatus.DRAFT, nullable=False, index=True)
    notes = Column(Text)
    
    account = relationship("Account", back_populates="invoices")
    entries = relationship(
        "AccountEntry",
        primaryjoin="AccountEntry.invoice_id==Invoice.id",
        foreign_keys=[AccountEntry.invoice_id],
        backref="invoice"
    )

    @property
    def total_amount(self):
        return sum(entry.amount for entry in self.entries)

    @property
    def is_overdue(self):
        return (
            self.due_date is not None and 
            self.status not in (InvoiceStatus.PAID, InvoiceStatus.CANCELLED) and
            datetime.utcnow() > self.due_date
        )

    def can_be_sent(self):
        return (
            self.status == InvoiceStatus.DRAFT and
            len(self.transactions) > 0 and
            self.due_date is not None
        )

    @validates('number')
    def validate_number(self, key, value):
        if not value:
            raise ValueError("Invoice number cannot be empty")
        return value

    def __repr__(self):
        return f"<Invoice {self.number}: {self.status.value}>"
