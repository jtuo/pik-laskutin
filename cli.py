from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from pik.models import Base, Aircraft, Flight
from pik.importer import DataImporter
import click
from config import Config

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
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

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
                if data_type == 'flights':
                    failed_rows = []
                    try:
                        count, failed_rows = self.importer.import_flights(
                            session=session,
                            filename=filename
                        )
                        session.commit()
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

    def list_flights(self, start_date=None, end_date=None):
        """List flights within the specified date range."""
        with self.session_scope() as session:
            try:
                # Implementation here
                pass
            except Exception as e:
                click.echo(f"Error listing flights: {str(e)}", err=True)
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
@click.option('--debug/--no-debug', default=False)
def cli(debug):
    """PIK Invoicing Software"""
    if debug:
        click.echo('Debug mode is on')
        # Configure debug logging
    pass


@cli.command(name='import')
@click.argument('type', type=click.Choice(['flights', 'nda']))
@click.argument('filename', type=click.Path(exists=True))
def import_data(type, filename):
    """Import data from CSV files.
    
    Supported types:
    - flights: CSV with date,pilot,aircraft,duration
    - nda: CSV with date,description,amount
    """
    try:
        invoicer = PIKInvoicer()
        invoicer.import_data(type, filename)
        click.echo(f"Successfully imported {type} data from {filename}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()


@cli.group()
def flights():
    """Commands for managing flights"""
    pass


@flights.command(name='list')
@click.option('--start-date', type=click.DateTime(), help='Start date for filtering flights')
@click.option('--end-date', type=click.DateTime(), help='End date for filtering flights')
def list_flights(start_date, end_date):
    """List all flights within the specified date range."""
    try:
        invoicer = PIKInvoicer()
        invoicer.list_flights(start_date, end_date)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
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
        click.echo(f"Error: {str(e)}", err=True)
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
        click.echo(f"Error: {str(e)}", err=True)
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
        click.echo(f"Error: {str(e)}", err=True)
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
        click.echo(f"Error: {str(e)}", err=True)
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
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()


@cli.command()
def status():
    """Show system status and statistics."""
    try:
        invoicer = PIKInvoicer()
        invoicer.show_status()
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    cli()
