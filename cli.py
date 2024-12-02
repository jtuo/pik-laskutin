from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from pik.models import Base, Aircraft, Flight, Account, Member, BaseEvent, Invoice, AccountEntry
from pik.rule_engine import create_default_engine
from pik.importer import DataImporter
from pik.util import get_caller_location
import click
from config import Config
import sys
import os

class PIKInvoicer:
    def __init__(self):
        self._validate_config()
        self.importer = DataImporter()
        
        self.db_url = Config.DB_URL
        self.engine = create_engine(self.db_url)
        Base.metadata.create_all(self.engine)
        self.SessionFactory = sessionmaker(bind=self.engine)

    def _validate_config(self):
        """Validate that all required configuration parameters are present."""
        required_configs = ['DB_URL']
        missing = [conf for conf in required_configs if not hasattr(Config, conf)]
        if missing:
            raise ValueError(f"Missing required configurations: {', '.join(missing)}")

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        caller_location = get_caller_location()
        session = self.get_session()
        logger.debug(f"Starting new database transaction from {caller_location}")
        try:
            yield session
            session.commit()
            logger.debug(f"Transaction committed successfully from {caller_location}")
        except Exception as e:
            session.rollback()
            logger.warning(f"Transaction rolled back due to error from {caller_location}: {str(e)}")
            raise
        finally:
            session.close()
            logger.debug(f"Database session closed from {caller_location}")

    def get_session(self) -> Session:
        """Get a new database session"""
        return self.SessionFactory()

    def export_invoice(self, invoice: Invoice, output_dir: str):
        """Export an invoice to a text file."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        filename = os.path.join(output_dir, f"{invoice.account.id}.txt")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"Invoice {invoice.number}\n")
            f.write(f"Account: {invoice.account.id} - {invoice.account.name}\n")
            created_date = invoice.created_at.strftime('%d.%m.%Y') if invoice.created_at else 'N/A'
            due_date = invoice.due_date.strftime('%d.%m.%Y') if invoice.due_date else 'N/A'
            f.write(f"Date: {created_date}\n")
            f.write(f"Due date: {due_date}\n\n")
            
            # Get entries from the invoice
            entries = invoice.entries
                
            f.write("Items:\n")
            f.write("-" * 60 + "\n")
            total = Decimal('0')
            for entry in entries:
                date_str = entry.date.strftime('%d.%m.%Y') if entry.date else 'N/A'
                # Right-justify the amount to 8 characters, then add description
                f.write(f"{date_str} {entry.amount:>8}€ - {entry.description}\n")
                total += entry.amount
            f.write("-" * 60 + "\n")
            f.write(f"Total: {total}€\n")

    def create_invoice(self, client: str, month: str):
        """Create an invoice for a client for a specific month."""
        with self.session_scope() as session:
            try:
                # Implementation here
                click.echo(f"Creating invoice for {client} for {month}")
            except Exception as e:
                click.echo(f"Error creating invoice: {str(e)}", err=True)
                raise

    def import_data(self, data_type: str, filename: str):
        """Import data from external files into the system."""
        with self.session_scope() as session:
            try:
                if data_type == 'members':
                    count, skipped = self.importer.import_members(
                        session=session,
                        filename=filename
                    )
                    click.echo(f"Successfully imported {count} members (skipped {skipped} existing)")
                elif data_type == 'flights':
                    failed_rows = []
                    try:
                        count, failed_rows = self.importer.import_flights(
                            session=session,
                            path_pattern=filename
                        )
                        click.echo(f"Successfully imported {count} records from {filename}")
                    except ValueError as e:
                        session.rollback()
                        click.echo(str(e), err=True)
                        if failed_rows:
                            for row, error in failed_rows:
                                click.echo(f"  Error: {error}", err=True)
                                click.echo(f"  Row data: {row}", err=True)
                        raise
                elif data_type == 'transactions':
                    self.importer.import_transactions(session, filename)
            except Exception as e:
                click.echo(f"Error during import: {str(e)}", err=True)
                raise

    def list_flights(self, start_date=None, end_date=None, aircraft_reg=None):
        """List flights within the specified date range and/or for specific aircraft."""
        with self.session_scope() as session:
            try:
                query = session.query(Flight).join(Aircraft)
                
                if start_date:
                    query = query.filter(Flight.date >= start_date)
                if end_date:
                    query = query.filter(Flight.date <= end_date)
                if aircraft_reg:
                    query = query.filter(Aircraft.registration == aircraft_reg.upper())
                    
                query = query.order_by(Flight.date, Flight.departure_time)
                
                flights = query.all()
                return [{
                    'date': flight.date,
                    'aircraft': flight.aircraft.registration,
                    'departure': flight.departure_time,
                    'landing': flight.landing_time,
                    'duration': float(flight.duration),
                    'reference': flight.reference_id,
                    'notes': flight.notes
                } for flight in flights]
            except Exception as e:
                click.echo(f"Error listing flights: {str(e)}", err=True)
                raise

    def count_flights(self, start_date=None, end_date=None, aircraft_reg=None):
        """Count flights within the specified date range and/or for specific aircraft."""
        with self.session_scope() as session:
            try:
                query = session.query(Flight).join(Aircraft)
                
                if start_date:
                    query = query.filter(Flight.date >= start_date)
                if end_date:
                    query = query.filter(Flight.date <= end_date)
                if aircraft_reg:
                    query = query.filter(Aircraft.registration == aircraft_reg.upper())
                    
                return query.count()
            except Exception as e:
                click.echo(f"Error counting flights: {str(e)}", err=True)
                raise

    def show_status(self):
        """Show system status and statistics."""
        with self.session_scope() as session:
            try:
                # Implementation here - could show total flights, invoices, etc.
                pass
            except Exception as e:
                click.echo(f"Error showing status: {str(e)}", err=True)
                raise
    
    def add_aircraft(self, registration: str, name: str, competition_id: str = None):
        """Add a new aircraft to the system."""
        with self.session_scope() as session:
            try:
                aircraft = Aircraft(
                    registration=registration,
                    name=name,
                    competition_id=competition_id
                )
                session.add(aircraft)
                session.flush()
                return {
                    'registration': aircraft.registration,
                    'name': aircraft.name,
                    'competition_id': aircraft.competition_id,
                    'id': aircraft.id
                }
            except Exception as e:
                click.echo(f"Error adding aircraft: {str(e)}", err=True)
                raise

    def list_aircraft(self):
        """List all aircraft in the system."""
        with self.session_scope() as session:
            try:
                aircraft = session.query(Aircraft).order_by(Aircraft.registration).all()
                return [{
                    'registration': a.registration,
                    'name': a.name,
                    'competition_id': a.competition_id,
                    'flight_count': len(a.flights)
                } for a in aircraft]
            except Exception as e:
                click.echo(f"Error listing aircraft: {str(e)}", err=True)
                raise

    def delete_aircraft(self, registration: str):
        """Delete an aircraft from the system."""
        with self.session_scope() as session:
            try:
                aircraft = session.query(Aircraft).filter(
                    Aircraft.registration == registration.upper()
                ).first()
                if not aircraft:
                    raise ValueError(f"Aircraft {registration} not found")
                
                if aircraft.flights:
                    raise ValueError(
                        f"Cannot delete aircraft {registration} as it has associated flights"
                    )
                
                session.delete(aircraft)
            except Exception as e:
                click.echo(f"Error deleting aircraft: {str(e)}", err=True)
                raise


@click.group()
@click.option('--no-debug', is_flag=True, default=False, help='Disable debug logging')
def cli(no_debug):
    """PIK Invoicing Software"""
    if no_debug:
        logger.remove()
        logger.add(sys.stderr, level="INFO")
    pass


@cli.command(name='import')
@click.argument('type', type=click.Choice(['flights', 'nda', 'members', 'transactions']))
@click.argument('filenames', nargs=-1, type=click.Path(exists=True))
def import_data(type, filenames):
    """Import data from CSV files.
    
    Supported types:
    - flights: CSV with date,pilot,aircraft,duration
    - nda: CSV with date,description,amount
    - members: CSV with reference_id,name,email,birth_date
    - transactions: CSV with bank transaction data
    """
    if not filenames:
        raise click.UsageError("At least one file must be specified")
        
    try:
        invoicer = PIKInvoicer()
        for filename in sorted(filenames):
            invoicer.import_data(type, filename)
    except Exception as e:
        logger.error(f"Error importing {type} data: {str(e)}")
        raise click.Abort()


@cli.group()
def flights():
    """Commands for managing flights"""
    pass


@flights.command(name='list')
@click.option('--start-date', type=click.DateTime(), help='Start date for filtering flights')
@click.option('--end-date', type=click.DateTime(), help='End date for filtering flights')
@click.option('--aircraft', help='Aircraft registration for filtering flights')
def list_flights(start_date, end_date, aircraft):
    """List all flights within the specified date range and/or for specific aircraft."""
    try:
        invoicer = PIKInvoicer()
        flights = invoicer.list_flights(start_date, end_date, aircraft)
        
        if not flights:
            click.echo("No flights found matching the criteria")
            return
            
        click.echo("\nFlight List:")
        click.echo("-" * 80)
        for f in flights:
            click.echo(f"Date: {f['date'].strftime('%Y-%m-%d')}")
            click.echo(f"Aircraft: {f['aircraft']}")
            click.echo(f"Time: {f['departure'].strftime('%H:%M')} - {f['landing'].strftime('%H:%M')}")
            click.echo(f"Duration: {f['duration']:.1f}h")
            click.echo(f"Reference: {f['reference']}")
            if f['notes']:
                click.echo(f"Notes: {f['notes']}")
            click.echo("-" * 80)
    except Exception as e:
        logger.error(f"Error listing flights: {str(e)}")
        raise click.Abort()


@flights.command(name='count')
@click.option('--start-date', type=click.DateTime(), help='Start date for filtering flights')
@click.option('--end-date', type=click.DateTime(), help='End date for filtering flights')
@click.option('--aircraft', help='Aircraft registration for filtering flights')
def count_flights(start_date, end_date, aircraft):
    """Count flights within the specified date range and/or for specific aircraft."""
    try:
        invoicer = PIKInvoicer()
        count = invoicer.count_flights(start_date, end_date, aircraft)
        
        # Build description of the count
        desc_parts = []
        if start_date:
            desc_parts.append(f"from {start_date.strftime('%Y-%m-%d')}")
        if end_date:
            desc_parts.append(f"to {end_date.strftime('%Y-%m-%d')}")
        if aircraft:
            desc_parts.append(f"for aircraft {aircraft}")
            
        desc = " ".join(desc_parts) if desc_parts else "total"
        
        click.echo(f"Number of flights {desc}: {count}")
    except Exception as e:
        logger.error(f"Error counting flights: {str(e)}")
        raise click.Abort()

@flights.command()
@click.option('--date', type=click.DateTime(), required=True, help='Flight date')
@click.option('--pilot', required=True, help='Pilot name')
@click.option('--aircraft', required=True, help='Aircraft registration')
@click.option('--duration', required=True, help='Flight duration')
def add(date, pilot, aircraft, duration):
    """Add a new flight manually."""
    try:
        invoicer = PIKInvoicer()
        with invoicer.session_scope() as session:
            # Implementation here
            click.echo(f"Added flight: {date} - {pilot} - {aircraft} - {duration}")
    except Exception as e:
        logger.error(f"Error adding flight: {str(e)}")
        raise click.Abort()

@cli.group()
def accounts():
    """Commands for managing accounts"""
    pass

@accounts.command(name='add')
@click.argument('reference_id')
@click.argument('name')
@click.option('--email', help='Email address for invoicing')
def add_account(reference_id, name, email):
    """Add a new account."""
    try:
        invoicer = PIKInvoicer()
        with invoicer.session_scope() as session:
            account = Account(
                id=reference_id,  # Using reference_id as primary key
                name=name,
                email=email
            )
            session.add(account)
            logger.info(f"Added new account: {reference_id} - {name} {'with email ' + email if email else ''}")
            click.echo(f"Added account: {reference_id} - {name}")
    except Exception as e:
        logger.error(f"Error adding account: {str(e)}")
        raise click.Abort()

@accounts.command(name='list')
def list_accounts():
    """List all accounts in the system."""
    try:
        invoicer = PIKInvoicer()
        with invoicer.session_scope() as session:
            accounts = session.query(Account).all()
            active_accounts = sum(1 for a in accounts if a.active)
            inactive_accounts = len(accounts) - active_accounts
            members_without_accounts = session.query(Member).filter(~Member.accounts.any()).count()

            click.echo("\nAccount Summary:")
            click.echo(f"Total accounts: {len(accounts)}")
            click.echo(f"Active accounts: {active_accounts}")
            click.echo(f"Inactive accounts: {inactive_accounts}")
            click.echo(f"Members without accounts: {members_without_accounts}")
    except Exception as e:
        logger.error(f"Error listing accounts: {str(e)}")
        raise click.Abort()

@accounts.command(name='create-missing-accounts')
def create_missing_member_accounts():
    """Create accounts for members who don't have accounts yet in the system."""
    try:
        invoicer = PIKInvoicer()
        with invoicer.session_scope() as session:
            members_without_accounts = session.query(Member).filter(~Member.accounts.any()).all()
            
            if not members_without_accounts:
                click.echo("All members already have accounts")
                return
                
            created_count = 0
            for member in members_without_accounts:
                account = Account(
                    id=member.id,  # Use member's PIK reference as account ID
                    member_id=member.id,
                    name=member.name + " lentotili"
                )
                session.add(account)
                created_count += 1
                logger.info(f"Created account for member {member.name} (ID: {member.id}) {'with email ' + member.email if member.email else ''}")
            
            click.echo(f"\nCreated {created_count} new accounts")
            
    except Exception as e:
        logger.error(f"Error creating member accounts: {str(e)}")
        raise click.Abort()

@cli.group()
def aircraft():
    """Commands for managing aircraft"""
    pass

@aircraft.command(name='list')
def list_aircraft():
    """List all aircraft in the system."""
    try:
        invoicer = PIKInvoicer()
        aircraft_list = invoicer.list_aircraft()
        if not aircraft_list:
            click.echo("No aircraft found in the system")
            return
        
        click.echo("\nAircraft List:")
        click.echo("-" * 50)
        for a in aircraft_list:
            click.echo(f"Registration: {a['registration']}")
            if a['name']:
                click.echo(f"Name: {a['name']}")
            if a['competition_id']:
                click.echo(f"Competition ID: {a['competition_id']}")
            click.echo(f"Number of flights: {a['flight_count']}")
            click.echo("-" * 50)
    except Exception as e:
        logger.error(f"Error listing aircraft: {str(e)}")
        raise click.Abort()

@aircraft.command()
@click.argument('registration')
@click.option('--name', '-n', help='Aircraft name (optional)')
@click.option('--competition-id', '-c', help='Competition ID (optional)')
def add(registration, name, competition_id):
    """Add a new aircraft.
    
    REGISTRATION: Aircraft registration (required)
    """
    try:
        invoicer = PIKInvoicer()
        aircraft_data = invoicer.add_aircraft(registration, name, competition_id)
        click.echo(f"Successfully added aircraft: {aircraft_data['registration']}")
    except Exception as e:
        logger.error(f"Error adding aircraft: {str(e)}")
        raise click.Abort()

@aircraft.command()
@click.argument('registration')
def delete(registration):
    """Delete an aircraft."""
    try:
        if not click.confirm(
            f"Are you sure you want to delete aircraft {registration}?"
        ):
            click.echo("Operation cancelled")
            return
        
        invoicer = PIKInvoicer()
        invoicer.delete_aircraft(registration)
        click.echo(f"Successfully deleted aircraft: {registration}")
    except Exception as e:
        logger.error(f"Error deleting aircraft: {str(e)}")
        raise click.Abort()

@cli.command()
@click.argument('client')
@click.argument('month')
def create_invoice(client, month):
    """Create an invoice for a client for a specific month."""
    try:
        invoicer = PIKInvoicer()
        invoicer.create_invoice(client, month)
        click.echo(f"Invoice created successfully for {client} - {month}")
    except Exception as e:
        logger.error(f"Error creating invoice: {str(e)}")
        raise click.Abort()


@cli.command()
@click.argument('account_id', required=False)
@click.option('--start-date', type=click.DateTime(), help='Start date for events')
@click.option('--end-date', type=click.DateTime(), help='End date for events')
@click.option('--dry-run', is_flag=True, help='Show what would be invoiced without creating invoices')
@click.option('--export', is_flag=True, help='Export invoices to text files')
def invoice(account_id, start_date, end_date, dry_run, export):
    """Generate invoices for uninvoiced events"""
    try:
        invoicer = PIKInvoicer()
        with invoicer.session_scope() as session:
            # Build query for uninvoiced events
            query = session.query(BaseEvent).filter(
                ~BaseEvent.account_entries.any(AccountEntry.event_id.isnot(None))
            )
            
            if account_id:
                query = query.filter(BaseEvent.account_id == account_id)
            if start_date:
                query = query.filter(BaseEvent.date >= start_date)
            if end_date:
                query = query.filter(BaseEvent.date <= end_date)
                
            # Order by account and date
            query = query.order_by(BaseEvent.account_id, BaseEvent.date)
            
            events = query.all()
            if not events:
                click.echo("No uninvoiced events found matching criteria")
                return
                
            # Process events through rule engine
            engine = create_default_engine()
            account_lines = engine.process_events(events, session)
            
            if dry_run:
                # Just show what would be created
                click.echo("\nDry run - would create these invoice lines:")
                for account, lines in account_lines.items():
                    click.echo(f"\nAccount: {account.id} - {account.name}")
                    total = Decimal('0')
                    for line in lines:
                        click.echo(f"  {line.date.strftime('%Y-%m-%d')} - {line.description}: {line.amount}€")
                        total += line.amount
                    click.echo(f"  Total: {total}€")
            else:
                # Create actual invoices
                for account, lines in account_lines.items():
                    # Create invoice
                    invoice = Invoice(
                        account=account,
                        number=f"INV-{datetime.now().strftime('%Y%m%d')}-{account.id}",
                        due_date=datetime.now() + timedelta(days=14)
                    )
                    session.add(invoice)
                    session.flush()  # Flush to get the invoice ID
                
                    # Add lines from rule engine to invoice
                    for line in lines:
                        entry = AccountEntry(
                            date=line.date,
                            account_id=account.id,
                            description=line.description,
                            amount=line.amount,
                            event_id=invoice.id  # Link to the invoice as the source event
                        )
                        session.add(entry)

                    # Find and add any uninvoiced AccountEntries for this account
                    uninvoiced_entries = session.query(AccountEntry).filter(
                        AccountEntry.account_id == account.id,
                        AccountEntry.invoice_id.is_(None)
                    ).all()
                
                    for entry in uninvoiced_entries:
                        entry.invoice_id = invoice.id  # Link existing entry to this invoice
                        
                    click.echo(f"Created invoice {invoice.number} for {account.id} with {len(lines)} lines")
                    
                    if export:
                        invoicer.export_invoice(invoice, "output")
                        click.echo(f"Exported invoice to output/{account.id}.txt")

    except Exception as e:
        logger.exception(f"Error creating invoices: {str(e)}")
        raise click.Abort()

@cli.command()
def status():
    """Show system status and statistics."""
    try:
        invoicer = PIKInvoicer()
        invoicer.show_status()
    except Exception as e:
        logger.error(f"Error showing status: {str(e)}")
        raise click.Abort()


if __name__ == '__main__':
    cli()
