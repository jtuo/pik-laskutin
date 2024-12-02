from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pik.models import Base, Aircraft
from config import Config
import click

def reset_db():
    """Drop and recreate all tables"""
    engine = create_engine(Config.DB_URL)
    
    # Drop all tables
    Base.metadata.drop_all(engine)
    
    # Recreate all tables
    Base.metadata.create_all(engine)
    
def populate_test_data():
    """Add sample data for development"""
    engine = create_engine(Config.DB_URL)
    Session = sessionmaker(bind=engine)
    
    with Session() as session:
        # Add some test aircraft
        test_aircraft = [
            Aircraft(registration="OH-952", name="DG"),
            Aircraft(registration="OH-733", name="Acro", competition_id="FQ"),
            Aircraft(registration="OH-787", name="LS-4", competition_id="FM")
        ]
        
        for aircraft in test_aircraft:
            session.add(aircraft)
        
        session.commit()

@click.command()
@click.option('--with-data/--no-data', default=True, help='Populate with test data')
def reset(with_data):
    """Reset database and optionally add test data"""
    click.echo("Resetting database...")
    reset_db()
    click.echo("Database reset complete")
    
    if with_data:
        click.echo("Adding test data...")
        populate_test_data()
        click.echo("Test data added")

if __name__ == '__main__':
    reset()
