from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, Numeric, Text, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, validates
from datetime import datetime
import enum
from config import Config

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
    invoices = relationship("Invoice", back_populates="account")

    def __repr__(self):
        return f"<Account {self.id}: {self.name}>"

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
        return f"<Aircraft {self.registration}>"

class Flight(Base):
    __tablename__ = 'flights'
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False, index=True)
    departure_time = Column(DateTime, nullable=False)
    landing_time = Column(DateTime, nullable=False)
    reference_id = Column(String(20), nullable=False, index=True)
    aircraft_id = Column(Integer, ForeignKey('aircraft.id'), nullable=False)
    account_id = Column(String(20), ForeignKey('accounts.id'), nullable=False)
    duration = Column(Numeric(5, 2), nullable=False)
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    aircraft = relationship("Aircraft", back_populates="flights")
    account = relationship("Account", back_populates="flights")
    invoice_lines = relationship("InvoiceLine", back_populates="flight")

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

    def __repr__(self):
        return f"<Flight {self.reference_id} on {self.date}>"

class InvoiceLine(Base):
    __tablename__ = 'invoice_lines'
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False, index=True)
    description = Column(Text, nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)
    flight_id = Column(Integer, ForeignKey('flights.id'), nullable=True, index=True)
    invoice_id = Column(Integer, ForeignKey('invoices.id'), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    flight = relationship("Flight", back_populates="invoice_lines")
    invoice = relationship("Invoice", back_populates="lines")

    @validates('amount')
    def validate_amount(self, key, value):
        if value <= 0:
            raise ValueError("Amount must be positive")
        return value

    def __repr__(self):
        return f"<InvoiceLine {self.description}: {self.amount}>"

class Invoice(Base):
    __tablename__ = 'invoices'
    
    id = Column(Integer, primary_key=True)
    number = Column(String(20), unique=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    account_id = Column(String(20), ForeignKey('accounts.id'), nullable=False)
    due_date = Column(DateTime)
    status = Column(Enum(InvoiceStatus), default=InvoiceStatus.DRAFT, nullable=False, index=True)
    notes = Column(Text)
    
    lines = relationship("InvoiceLine", back_populates="invoice", cascade="all, delete-orphan")
    account = relationship("Account", back_populates="invoices")

    @property
    def total_amount(self):
        return sum(line.amount for line in self.lines)

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
            len(self.lines) > 0 and
            self.recipient is not None and
            self.due_date is not None
        )

    @validates('number')
    def validate_number(self, key, value):
        if not value:
            raise ValueError("Invoice number cannot be empty")
        return value

    def __repr__(self):
        return f"<Invoice {self.number}: {self.status.value}>"
