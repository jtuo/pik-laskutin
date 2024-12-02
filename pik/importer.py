from sqlalchemy.orm import Session
from datetime import datetime
import csv
from decimal import Decimal
from .models import Flight, Aircraft
import logging

logger = logging.getLogger(__name__)

class DataImporter:
    def import_flights(self, session: Session, filename: str, progress_callback=None):
        """Import flight records from a CSV file.
        
        Args:
            session: SQLAlchemy session
            filename: Path to CSV file
            
        Expected CSV columns:
        Selite, Tapahtumapäivä, Maksajan viitenumero, Opettaja/Päällikkö,
        Oppilas/Matkustaja, Henkilöluku, Lähtöpaikka, Laskeutumispaikka,
        Lähtöaika, Laskeutumisaika, Lentoaika, Laskuja, Tarkoitus,
        Lentoaika_desimaalinen, Laskutuslisä syy, Pilveä
        """
        logger.info(f"Importing flights from {filename}")
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            count = 0
            for row in reader:
                try:
                    # Construct notes from available fields
                    notes_parts = []
                    if row.get('Opettaja/Päällikkö'):
                        notes_parts.append(f"Pilot: {row['Opettaja/Päällikkö']}")
                    if row.get('Oppilas/Matkustaja'):
                        notes_parts.append(f"Passenger: {row['Oppilas/Matkustaja']}")
                    if row.get('Tarkoitus'):
                        notes_parts.append(f"Purpose: {row['Tarkoitus']}")
                    if row.get('Laskutuslisä syy'):
                        notes_parts.append(f"Billing note: {row['Laskutuslisä syy']}")
                    
                    # Extract aircraft registration from Selite
                    registration = row['Selite'].split()[0]  # First word is registration
                    
                    # Find or create aircraft
                    aircraft = session.query(Aircraft).filter(
                        Aircraft.registration == registration.upper()
                    ).first()
                    if not aircraft:
                        aircraft = Aircraft(registration=registration)
                        session.add(aircraft)
                        session.flush()  # To get the aircraft.id
                    
                    flight = Flight(
                        date=datetime.strptime(row['Tapahtumapäivä'], '%Y-%m-%d'),
                        reference_number=row['Maksajan viitenumero'],
                        aircraft_id=aircraft.id,
                        duration=Decimal(row['Lentoaika_desimaalinen']),
                        notes='\n'.join(notes_parts) if notes_parts else None
                    )
                    session.add(flight)
                    count += 1
                except (KeyError, ValueError) as e:
                    logger.error(f"Error processing row: {row}. Error: {str(e)}")
                    raise
            
            logger.info(f"Successfully imported {count} flight records")
            if progress_callback:
                progress_callback(100)  # Complete the progress bar
