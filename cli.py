from contextlib import contextmanager
import inspect
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from pik.models import Base, Aircraft, Flight, Account, Member
from pik.importer import DataImporter
import click
from config import Config
import sys

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
        # Get caller information by walking up the stack
        frame = inspect.currentframe()
        try:
            # Walk up until we find a frame that's not from framework code
            caller_location = "unknown location"
            while frame:
                code = frame.f_code
                filename = code.co_filename
                function = code.co_name
                if (not any(x in filename.lower() for x in [
                    'contextlib.py', 
                    'click',
                    __file__
                ]) and function != 'session_scope'):
                    caller_info = inspect.getframeinfo(frame)
                    # Get just the module name without path and extension
                    module_name = caller_info.filename.split('\\')[-1].replace('.py', '')
                    caller_location = f"{module_name}:{caller_info.function}:{caller_info.lineno}"
                    break
                frame = frame.f_back
        finally:
            del frame  # Avoid reference cycles

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
                            filename=filename
                        )
                        click.echo(f"Successfully imported {count} records")
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
@click.argument('type', type=click.Choice(['flights', 'nda', 'members']))
@click.argument('filename', type=click.Path(exists=True))
def import_data(type, filename):
    """Import data from CSV files.
    
    Supported types:
    - flights: CSV with date,pilot,aircraft,duration
    - nda: CSV with date,description,amount
    - members: CSV with reference_id,name,email,birth_date
    """
    try:
        invoicer = PIKInvoicer()
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
                    name=member.name,
                    email=member.email
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
