from sqlalchemy.orm import Session
from datetime import datetime
import csv
from decimal import Decimal
from .models import Flight, Aircraft, Account, Member
from loguru import logger
from config import Config

class DataImporter:
    def import_members(self, session: Session, filename: str):
        """Import member records from a CSV file.
        
        Args:
            session: SQLAlchemy session
            filename: Path to CSV file
            
        Expected CSV columns:
        Jäsenen ID,Sukunimi,Etunimi,Sähköposti,Syntynyt,PIK-viite
        
        Returns:
            tuple: (number of imported records, number of skipped existing)
        """
        logger.info(f"Importing members from {filename}")
        count = 0
        skipped = 0
        
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Verify required columns
            required_columns = {'Sukunimi', 'Etunimi', 'PIK-viite'}
            missing_columns = required_columns - set(reader.fieldnames)
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
                
            for row in reader:
                try:
                    # Check if member already exists
                    existing = session.query(Member).filter(
                        Member.id == row['PIK-viite']
                    ).first()
                    
                    if existing:
                        skipped += 1
                        continue
                    
                    # Parse birth date if provided
                    birth_date = None
                    if row.get('Syntynyt'):
                        try:
                            # Try Finnish format first (DD.MM.YYYY)
                            birth_date = datetime.strptime(row['Syntynyt'], '%d.%m.%Y').date()
                        except ValueError:
                            try:
                                # Fall back to ISO format (YYYY-MM-DD)
                                birth_date = datetime.strptime(row['Syntynyt'], '%Y-%m-%d').date()
                            except ValueError as e:
                                raise ValueError(f"Invalid birth date format: {row['Syntynyt']}. Use DD.MM.YYYY or YYYY-MM-DD")
                    
                    # Create new member
                    member = Member(
                        id=row['PIK-viite'],
                        name=f"{row['Etunimi']} {row['Sukunimi']}",
                        email=row.get('Sähköposti'),
                        birth_date=birth_date
                    )
                    session.add(member)
                    count += 1
                    
                except Exception as e:
                    error_msg = f"Error in row {reader.line_num}: {str(e)}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                    
            return count, skipped

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
                    # Extract registration number from Selite
                    selite_reg = row['Selite'].split()[0].upper()  # First word is registration
                    
                    # Find aircraft where registration contains the Selite number
                    aircraft = session.query(Aircraft).filter(
                        Aircraft.registration.contains(selite_reg)
                    ).first()
                    
                    if not aircraft:
                        raise ValueError(f"Aircraft {selite_reg} not found in database")

                    # Find account by reference number if not a no-invoice reference
                    reference_id = row['Maksajan viitenumero']
                    account = None
                    account_id = None
                    
                    if reference_id not in Config.NO_INVOICING_REFERENCE_IDS:
                        account = session.query(Account).get(reference_id)
                        if not account:
                            raise ValueError(f"Account with reference ID {reference_id} not found")
                        account_id = account.id
                    
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
                        reference_id=row['Maksajan viitenumero'],
                        aircraft_id=aircraft.id,
                        account_id=account_id,
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
