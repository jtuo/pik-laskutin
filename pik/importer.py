from sqlalchemy.orm import Session
from datetime import datetime
import csv
from decimal import Decimal
from .models import Flight, Aircraft
from loguru import logger

class DataImporter:
    def import_flights(self, session: Session, filename: str):
        """Import flight records from a CSV file.
        
        Args:
            session: SQLAlchemy session
            filename: Path to CSV file
            
        Expected CSV columns:
        Selite, Tapahtumapäivä, Maksajan viitenumero, Opettaja/Päällikkö,
        Oppilas/Matkustaja, Henkilöluku, Lähtöpaikka, Laskeutumispaikka,
        Lähtöaika, Laskeutumisaika, Lentoaika, Laskuja, Tarkoitus,
        Lentoaika_desimaalinen, Laskutuslisä syy, Pilveä
        
        Returns:
            tuple: (number of imported records, list of failed rows with error messages)
            
        Raises:
            ValueError: If any row fails to import, the entire transaction is rolled back
        """
        logger.info(f"Importing flights from {filename}")
        flights_to_add = []
        count = 0
        failed_rows = []  # Initialize failed_rows at the start
        
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    # Extract aircraft registration from Selite
                    registration = row['Selite'].split()[0].upper()  # First word is registration
                    
                    # Find aircraft - must exist
                    aircraft = session.query(Aircraft).filter(
                        Aircraft.registration == registration
                    ).first()
                    
                    if not aircraft:
                        raise ValueError(f"Aircraft {registration} not found in database")
                    
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
                    
                    # Parse departure and landing times
                    date = datetime.strptime(row['Tapahtumapäivä'], '%Y-%m-%d')
                    departure_time = datetime.strptime(f"{row['Tapahtumapäivä']} {row['Lähtöaika']}", '%Y-%m-%d %H:%M')
                    landing_time = datetime.strptime(f"{row['Tapahtumapäivä']} {row['Laskeutumisaika']}", '%Y-%m-%d %H:%M')
                    
                    flight = Flight(
                        date=date,
                        departure_time=departure_time,
                        landing_time=landing_time,
                        reference_number=row['Maksajan viitenumero'],
                        aircraft_id=aircraft.id,
                        duration=Decimal(row['Lentoaika_desimaalinen']),
                        notes='\n'.join(notes_parts) if notes_parts else None
                    )
                    flights_to_add.append(flight)
                    count += 1
                    
                except Exception as e:
                    error_msg = f"Error in row {reader.line_num}: {str(e)}"
                    logger.error(error_msg)
                    failed_rows.append((row, error_msg))
                    continue

        if failed_rows:
            logger.warning(f"Failed to import {len(failed_rows)} rows")
            raise ValueError(f"Failed to import {len(failed_rows)} rows. No flights were imported.")
            
        # If we get here, all rows were valid - add them all
        for flight in flights_to_add:
            session.add(flight)
            
        return count, failed_rows
